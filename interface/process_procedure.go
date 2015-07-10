package mapreduce

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"io"
	"log"
	"strconv"
	"strings"

	pb "../proto"

	"golang.org/x/net/context"
)

func (t *workerTask) Emit(key, val string) {
	if t.config.ReducerNum == 0 {
		return
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	var KV mapperEmitKV
	KV.Key = key
	KV.Value = val
	toShuffle := h.Sum32() % uint32(t.config.ReducerNum)
	data, err := json.Marshal(KV)
	data = append(data, '\n')
	if err != nil {
		t.logger.Fatalf("json marshal error : ", err)
	}
	t.mapperWriteCloser[toShuffle].Write(data)
}

func (t *workerTask) Collect(key string, val string) {
	t.reducerWriteCloser.Write([]byte(key + " " + val + "\n"))
}

func (t *workerTask) Clean(path string) {
	err := t.config.FilesystemClient.Remove(path)
	if err != nil {
		t.logger.Println(err)
	}
}

func (t *workerTask) processShuffleKV(str []byte) {
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		t.shuffleContainer[tp.Key] = append(t.shuffleContainer[tp.Key], tp.Value)
	}
}

func (t *workerTask) mapperProcedure(ctx context.Context, workID string, workConfig WorkConfig, userClient pb.MapperStreamClient) {
	t.logger.Println("In mapper procedure")
	var i uint64
	t.mapperWriteCloser = make([]bufio.Writer, 0)

	for i = 0; i < t.config.ReducerNum; i++ {
		path := workConfig.OutputFilePath[0] + "/" + strconv.FormatUint(i, 10) + "from" + workID
		t.logger.Println("Output Path ", path)
		t.Clean(path)
		tmpWrite, err := t.config.FilesystemClient.OpenWriteCloser(path)
		if err != nil {
			t.logger.Fatalf("MapReduce : get mapreduce filesystem client writer failed, ", err)
		}
		t.mapperWriteCloser = append(t.mapperWriteCloser, *bufio.NewWriterSize(tmpWrite, t.config.WriterBufferSize))
	}

	waitc := make(chan struct{})
	stream, err := userClient.GetStreamEmitResult(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			in, err := stream.Recv()
			// t.logger.Println(in)
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to receive a note : %v", err)
				return
			}
			if err == io.EOF || (len(in.Arr) == 1 && in.Arr[0].Key == "Stop" && in.Arr[0].Value == "Stop") {
				// read done.
				close(waitc)
				return
			}
			for _, v := range in.Arr {
				t.Emit(v.Key, v.Value)
			}
		}
	}()

	var buf []*pb.KvPair

	// Input file loading
	for readFileID := 0; readFileID < len(workConfig.InputFilePath); readFileID++ {
		mapperReaderCloser, err := t.config.FilesystemClient.OpenReadCloser(workConfig.InputFilePath[readFileID])
		if err != nil {
			t.logger.Fatalf("MapReduce : get mapreduce filesystem client reader failed, ", err)
		}

		var str string
		bufioReader := bufio.NewReaderSize(mapperReaderCloser, t.config.ReaderBufferSize)

		for err != io.EOF {
			str, err = bufioReader.ReadString('\n')
			if err != io.EOF && err != nil {
				t.logger.Fatalf("MapReduce : mapper read Error, ", err)
			}
			if err != io.EOF {
				str = str[:len(str)-1]
			}
			buf = append(buf, &pb.KvPair{Key: str, Value: ""})
			if len(buf) >= bufferSize {
				stream.Send(&pb.MapperRequest{buf})
				buf = nil
			}
		}
		// stop the reader of corresponding file
		mapperReaderCloser.Close()
	}

	// stop user program grpc client
	stream.Send(&pb.MapperRequest{buf})
	stream.CloseSend()
	<-waitc
	//flush output result
	for i = 0; i < t.config.ReducerNum; i++ {
		t.mapperWriteCloser[i].Flush()
	}
	t.logger.Println("FileRead finished")

	// notify the master mapper work has been done
	t.notifyChan <- &mapreduceEvent{ctx: ctx, fromID: t.taskID, linkType: "Master", meta: "WorkFinished" + workID}
}

func (t *workerTask) reducerProcedure(ctx context.Context, workID string, workConfig WorkConfig, userClient pb.ReducerStreamClient) {
	waitc := make(chan struct{})
	stream, err := userClient.GetStreamCollectResult(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to receive a note : %v", err)
			}
			if err == io.EOF || (len(in.Arr) == 1 && in.Arr[0].Key == "Stop" && in.Arr[0].Value == "Stop") {

				close(waitc)
				return
			}
			for _, v := range in.Arr {
				t.Collect(v.Key, v.Value)
			}
		}
	}()
	reducerOutputPath := workConfig.OutputFilePath[0]
	t.Clean(reducerOutputPath)

	reducerWriteCloser, err := t.config.FilesystemClient.OpenWriteCloser(reducerOutputPath)
	t.reducerWriteCloser = *bufio.NewWriterSize(reducerWriteCloser, t.config.WriterBufferSize)

	var buf []*pb.KvsPair

	for ProcessID := 0; ProcessID < len(workConfig.InputFilePath); ProcessID++ {
		t.shuffleContainer = make(map[string][]string)

		arg := strings.Split(workConfig.SupplyContent[ProcessID], " ")

		mapperWorkSum, err := strconv.ParseUint(arg[0], 10, 64)
		if err != nil {
			t.logger.Fatalf("Failed to get argv mapperWorkSum : %v", err)
		}
		t.logger.Println(workConfig.InputFilePath)
		t.logger.Println(workConfig.OutputFilePath)
		for i := uint64(0); i < mapperWorkSum; i++ {
			shufflePath := workConfig.InputFilePath[0] + "/" + arg[1] + "from" + strconv.FormatUint(i, 10)
			shuffleReadCloser, err := t.config.FilesystemClient.OpenReadCloser(shufflePath)
			t.logger.Println("get shuffle data from ", shufflePath)
			if err != nil {
				if strings.Contains(err.Error(), "The specified blob does not exist") {
					continue
				}
				t.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
			}
			bufioReader := bufio.NewReaderSize(shuffleReadCloser, t.config.ReaderBufferSize)
			var str []byte
			err = nil
			for err != io.EOF {
				str, err = bufioReader.ReadBytes('\n')
				if err != io.EOF && err != nil {
					t.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
				}
				if err != io.EOF {
					str = str[:len(str)-1]
				}
				t.processShuffleKV(str)
			}
		}
		if err != nil {
			t.logger.Fatalf("MapReduce : get reducer writer error, %v", err)
		}

		for k := range t.shuffleContainer {
			// t.collectKvPairs(userClient, k, t.shuffleContainer[k], false)
			buf = append(buf, &pb.KvsPair{Key: k, Value: t.shuffleContainer[k]})
			if len(buf) >= bufferSize {
				stream.Send(&pb.ReducerRequest{buf})
				buf = nil
			}
		}

	}
	stream.Send(&pb.ReducerRequest{buf})
	stream.CloseSend()
	<-waitc
	t.reducerWriteCloser.Flush()
	t.logger.Println("finshed reducer")
	t.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: t.epoch, linkType: "Master", meta: "WorkFinished" + workID}
	// for i := uint64(0); i < mapperWorkSum; i++ {
	// 	shufflePath := workConfig.InputFilePath[i] + "/" + arg[1] + "from" + strconv.FormatUint(i, 10)
	// 	t.Clean(shufflePath)
	// }
}
