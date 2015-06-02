package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	pb "./proto"
	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type workerTask struct {
	framework          taskgraph.Framework
	taskType           string
	epoch              uint64
	logger             *log.Logger
	taskID             uint64
	workID             uint64
	etcdClient         *etcd.Client
	userSeverPort      uint64
	mapperWriteCloser  []bufio.Writer
	reducerWriteCloser bufio.Writer
	//channels
	epochChange               chan *mapreduceEvent
	dataReady                 chan *mapreduceEvent
	metaReady                 chan *mapreduceEvent
	finishedChan              chan *mapreduceEvent
	notifyChan                chan *mapreduceEvent
	exitChan                  chan struct{}
	stopGrabTaskForEveryEpoch chan bool

	//io writer
	shuffleDepositWriter bufio.Writer

	config MapreduceConfig
}

type mapperEmitKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (t *workerTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	t.taskID = taskID
	t.framework = framework
	t.etcdClient = etcd.NewClient(t.config.EtcdURLs)
	//channel init
	t.stopGrabTaskForEveryEpoch = make(chan bool, 1)
	t.epochChange = make(chan *mapreduceEvent, 1)
	t.dataReady = make(chan *mapreduceEvent, 1)
	t.metaReady = make(chan *mapreduceEvent, 1)
	t.exitChan = make(chan struct{})
	t.initializeTaskEnv()
	go t.run()
}

func (t *workerTask) run() {
	for {
		select {
		case ec := <-t.epochChange:
			go t.doEnterEpoch(ec.ctx, ec.epoch)

		case notify := <-t.notifyChan:
			t.framework.FlagMeta(notify.ctx, notify.linkType, notify.meta)

		case <-t.exitChan:
			return

		case dataReady := <-t.dataReady:
			go t.processWork(dataReady.ctx, dataReady.fromID, t.workID, dataReady.method, dataReady.output)
		case metaReady := <-t.metaReady:

		}
	}
}

func (t *workerTask) startNewUserServer(cmd []string) {
	argv := []string{"-port=" + strconv.FormatUint(t.taskID+10000, 10)}
	// c := exec.Command(cmd[0], argv)

}

func (t *workerTask) getNewMapperUserServer(address string) pb.MapperClient {
	conn, err := grpc.Dial(address + fmt.Sprintf(":%d", t.userSeverPort))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewMapperClient(conn)
}

func (t *workerTask) getNewReducerUserServer(address string) pb.ReducerClient {
	conn, err := grpc.Dial(address + fmt.Sprintf(":%d", t.userSeverPort))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewReducerClient(conn)
}

func (t *workerTask) processWork(ctx context.Context, fromID uint64, workID uint64, method string, output proto.Message) {
	resp, ok := output.(*pb.WorkConfigResponse)
	if !ok {
		t.logger.Panicf("doDataRead, corruption in proto.Message.")
	}
	pair := make(map[string]string)
	workConfig := WorkConfig{}
	for i := range resp.Key {
		switch i {
		case "InputFilePath":
			workConfig.InputFilePath = resp.Value[i]
		case "OutputFilePath":
			workConfig.OutputFilePath = resp.Value[i]
		case "UserProgram":
			workConfig.UserProgram = resp.Value[i]
		case "UserServerAddress":
			workConfig.UserServerAddress = resq.Value[i]
		case "WorkType":
			workConfig.WorkType = resp.Value[i]
		case "SupplyContent":
			workConfig.SupplyContent = resp.Value[i]
		}
	}

	// start user grpc server by cmd line,
	startNewUserServer(workConfig.UserProgram)

	// start relative processing procedure
	switch pair["WorkType"] {
	case "Mapper":
		userClient := t.getNewMapperUserServer(workConfig.UserServerAddress)
		go t.mapperProcedure(ctx, workID, workConfig, userClient)
	case "Reducer":
		userClient := t.getNewReducerUserServer(workConfig.UserServerAddress)
		go t.reducerProcedure(ctx, workID, workConfig, userClient)
	}
}

func (t *workerTask) initializeTaskEnv() error {
	_, err := t.etcdClient.Create(MapreduceNodeStatusPath(t.config.AppName, t.taskID, "workStatus"), "non", 0)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
			return nil
		}
		return err
	}
}

func (t *workerTask) datarequestForWork(ctx context.Context, method string) {
	master := t.framework.GetTopology()["Master"].GetNeighbors(mp.epoch)

	for _, node := range master {
		t.framework.DataRequest(ctx, node, method, &pb.Request{taskID: mp.taskID})
	}
}

func (t *workerTask) grabWork(ctx context.Context, method string, stop chan bool) {
	datarequestForWork(ctx, method)

	// afterwards, watch etcd worker attribute "workStatus"
	// if exist "set' operation, and the value is "non"
	receiver := make(chan *etcd.Response, 1)
	go client.Watch(MapreduceNodeStatusPath(t.config.AppName, t.taskID, "workStatus"), 0, false, receiver, stop)
	for resp := range receiver {
		if resp.Action != "set" {
			continue
		}
		if resp.Node.Value == "non" {
			datarequestForWork(ctx, method)
		}
	}
}

func (t *workerTask) Emit(key, val string) {
	if mp.config.ReducerNum == 0 {
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
	err := mp.mapreduceConfig.FilesystemClient.Remove(path)
	if err != nil {
		mp.logger.Fatal(err)
	}
}

func (t *workerTask) emitKvPairs(userClient pb.MapperClient, str string, value string, stop chan bool) {
	stream, err := userClient.GetEmitResult(context.Background(), &pb.MapperRequest{Key: str, Value: v})
	if err != nil {
		t.logger.Fatalf("could not access the user program server : %v", err)
	}
	if !stop {
		for {
			feature, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("%v.GetEmitResult, %v", userClient, err)
				return
			}
			t.Emit(feature.Key, feature.Value)
		}
	}
}

func (t *workerTask) mapperProcedure(ctx context.Context, workID uint64, workConfig WorkConfig, userClient pb.MapperClient) {
	var i uint64
	t.mapperWriteCloser = make([]bufio.Writer, 0)

	for i = 0; i < t.config.ReducerNum; i++ {
		path := workConfig.OutputFilePath + "/" + strconv.FormatUint(i, 10) + "from" + strconv.FormatUint(workID, 10)
		t.logger.Println("Output Path ", path)
		t.Clean(path)
		tmpWrite, err := t.config.FilesystemClient.OpenWriteCloser(path)
		if err != nil {
			t.logger.Fatalf("MapReduce : get mapreduce filesystem client writer failed, ", err)
		}
		t.WriteCloser = append(mp.mapperWriteCloser, *bufio.NewWriterSize(tmpWrite, t.config.WriterBufferSize))
	}

	// Input file loading
	for readFileID := 0; readFileID < len(workConfig.InputFilePath); readFileID++ {
		mapperReaderCloser, err := t.FilesystemClient.OpenReadCloser(workConfig.InputFilePath[readFileID])
		if err != nil {
			t.logger.Fatalf("MapReduce : get mapreduce filesystem client reader failed, ", err)
		}

		var str string
		bufioReader := bufio.NewReaderSize(mapperReaderCloser, mp.config.ReaderBufferSize)

		for err != io.EOF {
			str, err = bufioReader.ReadString('\n')

			if err != io.EOF && err != nil {
				mp.logger.Fatalf("MapReduce : mapper read Error, ", err)
			}
			if err != io.EOF {
				str = str[:len(str)-1]
			}
			t.emitKvPairs(userClient, str, "", false)
		}
		// stop the reader of corresponding file
		mapperReaderCloser.Close()
	}

	// stop user program grpc client
	t.emitKvPairs(userClient, "stop", "stop", true)

	//flush output result
	for i = 0; i < mp.mapreduceConfig.ReducerNum; i++ {
		t.mapperWriteCloser[i].Flush()
	}
	t.logger.Println("FileRead finished")

	// notify the master mapper work has been done
	t.notifyChan <- &mapreduceEvent{ctx: ctx, workID: mp.workID, fromID: mp.taskID, linkType: "Master", meta: "WorkFinished" + strconv.FormatUint(mp.workID, 10)}
}

func (t *workerTask) processShuffleKV(str []byte) {
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		t.shuffleContainer[tp.Key] = append(t.shuffleContainer[tp.Key], tp.Value)
	}
}

func (t *workerTask) collectKvPairs(userClient pb.ReducerClient, key string, value []string, stop bool) {
	r, err := userClient.GetCollectResult(context.Background(), &pb.ReducerRequest{Key: key, Value: value})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	if !stop {
		for {
			feature, err := r.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("%v.GetCollectResult, %v", userClient, err)
				return
			}
			t.Collect(feature.Key, feature.Value)
		}
	}
}

func (t *workerTask) reducerProcedure(ctx context.Context, workID uint64, workConfig WorkConfig, userClient pb.ReducerClient) {
	for ProcessID := 0; ProcessID < len(workConfig.InputFilePath); ProcessID++ {
		t.shuffleContainer = make(map[string][]string)

		arg := strings.Split(workConfig.SupplyContent[ProcessID], " ")

		mapperWorkSum, err := strconv.ParseUint(arg[0], 10, 64)
		if err != nil {
			t.logger.Fatalf("Failed to get argv mapperWorkSum : %v", err)
		}

		reducerID, err := strconv.ParseUint(arg[1], 10, 64)
		if err != nil {
			t.logger.Fatalf("Failed to get argv reducerID : %v", err)
		}

		for i := 0; i < mapperWorkSum; i++ {
			shufflePath := workConfig.InputFilePath[i] + "/" + arg[1] + "from" + strconv.Itoa(i)
			shuffleReadCloser, err := t.config.FilesystemClient.OpenReadCloser(shufflePath)
			t.logger.Println("get shuffle data from ", shufflePath)
			if err != nil {
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

		t.Clean(tranferPath)

		reducerWriteCloser, err := t.config.FilesystemClient.OpenWriteCloser(workConfig.OutputFilePath[ProcessID])
		if err != nil {
			t.logger.Fatalf("MapReduce : get reducer writer error, %v", err)
		}

		t.reducerWriteCloser = bufio.NewWriterSize(reducerWriteCloser, t.config.WriterBufferSize)

		for k := range mp.shuffleContainer {
			t.collectKvPairs(k, t.shuffleContianer[k], false)
		}
		t.reducerWriteCloser.Flush()
	}

	t.collectKvPairs("Stop", []string{}, true)

	t.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: t.epoch, linkType: "Master", meta: "ReducerWorkFinished" + strconv.FormatUint(mp.workID, 10)}
}

func (t *workerTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &mapreduceEvent{ctx: ctx, epoch: epoch}
}

func (t *workerTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	// stop the last epoch grab work procedure
	// start a new one
	close(t.stopGrabTaskForEveryEpoch)
	t.stopGrabTaskForEveryEpoch = make(chan bool, 1)
	grabWork(ctx, "/proto.Master/GetWork", t.stopGrabTaskForEveryEpoch)
}

func (t *workerTask) Exit() {
	close(t.stopGrabTaskForEveryEpoch)
	close(t.exitChan)
}

func (t *workerTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, t)
	return server

}

func (t *workerTask) CreateOutputMessage(method string) proto.Message {
	return nil
}

func (t *workerTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.dataReady <- &event{ctx: ctx, fromID: fromID, method: method, output: output}
}

func (t *workerTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	t.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}
