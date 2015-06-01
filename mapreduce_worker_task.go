package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"math"
	"os"
	"strconv"

	pb "./proto"
	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/pkg/etcdutil"
	"github.com/taskgraph/taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const nonExistWork = math.MaxUint64

type workerTask struct {
	framework taskgraph.Framework
	taskType  string
	epoch     uint64
	logger    *log.Logger
	taskID    uint64
	workID    uint64

	//channels
	epochChange  chan *mapreduceEvent
	dataReady    chan *mapreduceEvent
	metaReady    chan *mapreduceEvent
	finishedChan chan *mapreduceEvent
	notifyChan   chan *mapreduceEvent
	exitChan     chan struct{}
	stopGrabTaskForEveryEpoch chan bool

	//io writer
	shuffleDepositWriter bufio.Writer

	config MapreduceConfig
}

func (t *workerTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	t.taskID = taskID
	t.framework = framework
	t.finishedTask = make(map[uint64]bool)
	//channel init
	t.stopGrabTask = make(chan bool, 1)
	t.epochChange = make(chan *mapreduceEvent, 1)
	t.dataReady = make(chan *mapreduceEvent, 1)
	t.metaReady = make(chan *mapreduceEvent, 1)
	t.exitChan = make(chan struct{})
	t.initializeTaskEnv()
	go t.run()
}

func (mp *mapreduceTask) run() {
	for {
		select {
		case ec := <-mp.epochChange:
			go mp.doEnterEpoch(ec.ctx, ec.epoch)

		case notify := <-mp.notifyChan:
			mp.framework.FlagMeta(notify.ctx, notify.linkType, notify.meta)

		case <-mp.exitChan:
			return

		case dataReady := <-dataReady:
			go mp.processWork(dataReady.ctx, dataReady.fromID, dataReady.method, dataReady.output)
		}
	}
}

func (t *workerTask) processWork(ctx context.Context, fromID uint64, method string, message proto.Message) {
	resp, ok := output.(*pb.Response)
	if !ok {
		t.logger.Panicf("doDataRead, corruption in proto.Message.")
	}
	pair := make(map[string]string)
	for i := range resp.Key {
		pair[resp.Key[i]] = resp.Value[i]
	}
	workConfig := WorkConfig{
		InputFilePath  : pair["InputFilePath"],
		OutputFilePath : pair["OutputFilePath"],
		UserProgram  : pair["UserProgram"],
		WorkType : pair["WorkType"],
	}



	switch pair["WorkType"] {
		case "Mapper" :
			go t.mapperProcedure(ctx, workConfig)
		case "Reducer" :
			go t.reducerProcedure(ctx, workConfig)

	}
}

func (t *workerTask) initializeTaskEnv() error {
	_, err := t.etcdClient.Create((name, taskID), "non", 10)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
			return nil
		}
		return err
	}
}

func (t *workerTask) grabTask(stop *chan bool) {
	// To begin with, check the initial work state of this task
	// if no work exist, grab new one


	// afterwards, watch etcd worker attribute "workStatus"
	// if exist operation 


}

func (t *workerTask) emitKvPairs(userClient pb.MapperClient, str string, value string, stop) {
	stream, err := c.GetEmitResult(context.Background(), &pb.MapperRequest{Key: str, Value: v})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	if !stop {
		fmt.Println("in Emit steps")
		for {
			feature, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("%v.ListFeatures(_) = _, %v", c, err)
				return
			}
			fmt.Println(feature)
		}
	}
}



func (t *workerTask) mapperProcedure(ctx context.Context, workID uint64, workConfig WorkConfig, userClient pb.MapperClient) {
	var i uint64
	t.mapperWriteCloser = make([]bufio.Writer, 0)

	for i = 0; i < mp.config.ReducerNum; i++ {
		path := workConfig.OutputFilePath + "/" + strconv.FormatUint(i, 10) + "from" + strconv.FormatUint(workID, 10)
		t.logger.Println("Output Path ", path)
		t.Clean(path)
		tmpWrite, err := t.config.FilesystemClient.OpenWriteCloser(path)
		if err != nil {
			t.logger.Fatalf("MapReduce : get azure storage client writer failed, ", err)
		}
		t.WriteCloser = append(mp.mapperWriteCloser, *bufio.NewWriterSize(tmpWrite, mp.mapreduceConfig.WriterBufferSize))
	}


	// Input file loading
	mapperReaderCloser, err := t.FilesystemClient.OpenReadCloser(workConfig.InputFilePath)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client reader failed, ", err)
	}
	var str string

	bufioReader := bufio.NewReaderSize(mapperReaderCloser, mp.config.ReaderBufferSize)

	for err != io.EOF {
		str, err = bufioReader.ReadString('\n')

		if err != io.EOF && err != nil {
			mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		t.emitKvPairs(userClient, str, "", false)
	}
	// stop the reader and user program grpc client
	mapperReaderCloser.Close()
	t.emitKvPairs(userClient, "stop", "stop", true)

	//flush output result
	for i = 0; i < mp.mapreduceConfig.ReducerNum; i++ {
		t.mapperWriteCloser[i].Flush()
	}
	t.logger.Println("FileRead finished")
	// notify the master mapper work has been done
	t.notifyChan <- &mapreduceEvent{ctx: ctx, workID: mp.workID, fromID: mp.taskID, linkType: "Master", meta: "WorkFinished" + strconv.FormatUint(mp.workID, 10)}
}

// Read mapper data, shuffle, set a new reducer work
func (mp *mapreduceTask) transferShuffleData(ctx context.Context) {
	mp.logger.Println("In transfer Data function")
	mp.shuffleContainer = make(map[string][]string)
	workNum := len(mp.mapreduceConfig.WorkDir["mapper"])
	for i := 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.Itoa(i)
		shuffleReadCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(shufflePath)
		mp.logger.Println("get shuffle data from ", shufflePath)
		if err != nil {
			mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		}
		bufioReader := bufio.NewReaderSize(shuffleReadCloser, mp.mapreduceConfig.ReaderBufferSize)
		var str []byte
		err = nil
		for err != io.EOF {
			str, err = bufioReader.ReadBytes('\n')
			if err != io.EOF && err != nil {
				mp.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			}
			if err != io.EOF {
				str = str[:len(str)-1]
			}
			mp.processShuffleKV(str)
		}
	}

	tranferPath := mp.mapreduceConfig.InterDir + "/shuffle" + strconv.FormatUint(mp.workID, 10)
	mp.Clean(tranferPath)
	mp.logger.Println("output shuffle data to ", tranferPath)
	shuffleWriteCloser, err := mp.mapreduceConfig.FilesystemClient.OpenWriteCloser(tranferPath)
	bufferWriter := bufio.NewWriterSize(shuffleWriteCloser, mp.mapreduceConfig.WriterBufferSize)
	if err != nil {
		mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
	}
	for k := range mp.shuffleContainer {
		block := &shuffleEmit{
			Key:   k,
			Value: mp.shuffleContainer[k],
		}
		data, err := json.Marshal(block)
		if err != nil {
			mp.logger.Fatalf("Shuffle Emit json error, %v\n", err)
		}
		data = append(data, '\n')
		bufferWriter.Write(data)
	}
	bufferWriter.Flush()
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType: "Slave", meta: "ShuffleWorkFinished"}
	key := etcdutil.FreeWorkPathForType(mp.mapreduceConfig.AppName, "reducer", strconv.FormatUint(mp.workID, 10))
	mp.logger.Println("shuffle finished, add reducer work ", key)

	mp.etcdClient.Set(key, "begin", 0)
	for i := 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.Itoa(i)
		mp.Clean(shufflePath)
	}
	mp.etcdClient.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, strconv.FormatUint(mp.taskID, 10)), false)

}

func (mp *mapreduceTask) getNodeTaskType() string {
	master := len(mp.framework.GetTopology().GetNeighbors("Master", mp.epoch))
	if master == 0 {
		return "master"
	}
	switch mp.epoch {
	case 0:
		if mp.taskID < mp.mapreduceConfig.MapperNum {
			return "mapper"
		}
		return "shuffle"
	case 1:
		if mp.taskID < mp.mapreduceConfig.ShuffleNum {
			return "shuffle"
		}
		return "reducer"
	}
	return "reducer"
}

func (mp *mapreduceTask) reducerProcess(ctx context.Context) {
	outputPath := mp.mapreduceConfig.OutputDir + "/reducer" + strconv.FormatUint(mp.workID, 10)
	mp.Clean(outputPath)
	outputWrite, err := mp.mapreduceConfig.FilesystemClient.OpenWriteCloser(outputPath)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	mp.outputWriter = bufio.NewWriterSize(outputWrite, mp.mapreduceConfig.WriterBufferSize)
	reducerPath := mp.mapreduceConfig.InterDir + "/shuffle" + strconv.FormatUint(mp.workID, 10)

	reducerReadCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(reducerPath)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReaderSize(reducerReadCloser, mp.mapreduceConfig.ReaderBufferSize)
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		if err != io.EOF && err != nil {
			mp.logger.Fatalf("MapReduce : Reducer read Error, ", err)
			return
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		mp.processReducerKV(str)
	}
	mp.outputWriter.Flush()
	mp.logger.Printf("%s removing..\n", reducerPath)
	mp.Clean(reducerPath)
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType: "Slave", meta: "ReducerWorkFinished" + strconv.FormatUint(mp.workID, 10)}
	mp.etcdClient.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, strconv.FormatUint(mp.taskID, 10)), false)

}

func (mp *workerTask) EnterEpoch(ctx context.Context, epoch uint64) {
	mp.epochChange <- &mapreduceEvent{ctx: ctx, epoch: epoch}
}

func (mp *workerTask) grabWork(ctx context.Context, method string) {
	master := t.framework.GetTopology()["Master"].GetNeighbors(mp.epoch)

	for _, node := range master {
		t.framework.DataRequest(ctx, node, method, &pb.Request{taskID: mp.taskID})
	}
}

func (mp *mapreduceTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	grabWork(ctx, "/proto.Master/GetWork")
}

func (mp *mapreduceTask) Exit() {
	close(mp.stopGrabTask)
	close(mp.exitChan)
}

func (mp *mapreduceTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, mp)
	return server

}

func (mp *mapreduceTask) CreateOutputMessage(method string) proto.Message { return nil }

func (mp *mapreduceTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.dataReady <- &event{ctx: ctx, fromID: fromID, method: method, output: output}
}

func (m *mapreduceTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	m.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}
