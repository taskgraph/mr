package mapreduce

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"
	pb "github.com/plutoshe/mr/proto"
	"github.com/plutoshe/taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const bufferSize = 5000

type workerTask struct {
	framework          taskgraph.Framework
	epoch              uint64
	logger             *log.Logger
	taskID             uint64
	etcdClient         *etcd.Client
	userSeverPort      uint64
	mapperWriteCloser  []bufio.Writer
	reducerWriteCloser bufio.Writer
	shuffleContainer   map[string][]string //store temporary shuffle results

	//channels
	epochChange               chan *mapreduceEvent
	dataReady                 chan *mapreduceEvent
	metaReady                 chan *mapreduceEvent
	notifyChan                chan *mapreduceEvent
	exitChan                  chan struct{}
	stopGrabTaskForEveryEpoch chan bool

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
	t.userSeverPort = 10000 + taskID

	//channel init
	t.stopGrabTaskForEveryEpoch = make(chan bool, 1)
	t.epochChange = make(chan *mapreduceEvent, 1)
	t.dataReady = make(chan *mapreduceEvent, 1)
	t.metaReady = make(chan *mapreduceEvent, 1)
	t.notifyChan = make(chan *mapreduceEvent, 1)
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
			go t.processWork(dataReady.ctx, dataReady.fromID, dataReady.method, dataReady.output)
		case <-t.metaReady:

		}
	}
}

func (t *workerTask) processWork(ctx context.Context, fromID uint64, method string, output proto.Message) {

	resp, ok := output.(*pb.WorkConfigResponse)
	t.logger.Println(resp)
	if !ok {
		t.logger.Panicf("doDataRead, corruption in proto.Message.")
	}
	if resp == nil {
		return
	}
	workID := t.getWorkID()
	if workID == "non" {
		t.logger.Println("Expect worker possessing a work")
		return
	}

	workConfig := WorkConfig{}
	for i := range resp.Key {
		switch resp.Key[i] {
		case "InputFilePath":
			workConfig.InputFilePath = strings.Split(resp.Value[i], delim)
		case "OutputFilePath":
			workConfig.OutputFilePath = strings.Split(resp.Value[i], delim)
		case "UserProgram":
			workConfig.UserProgram = strings.Split(resp.Value[i], delim)
		case "UserServerAddress":
			workConfig.UserServerAddress = resp.Value[i]
		case "WorkType":
			workConfig.WorkType = resp.Value[i]
		case "SupplyContent":
			workConfig.SupplyContent = strings.Split(resp.Value[i], delim)
		}
	}
	// start user grpc server by cmd line,
	t.startNewUserServer(workConfig.UserProgram)
	t.logger.Println("begin process work", workConfig)
	t.logger.Println(workConfig.WorkType)
	// Determined by the work type, start relative processing procedure
	switch workConfig.WorkType {
	case "Mapper":
		userClient := t.getNewMapperUserServer(workConfig.UserServerAddress)
		go t.mapperProcedure(ctx, workID, workConfig, userClient)
	case "Reducer":
		userClient := t.getNewReducerUserServer(workConfig.UserServerAddress)
		go t.reducerProcedure(ctx, workID, workConfig, userClient)
	}
}

func (t *workerTask) initializeTaskEnv() error {
	_, err := t.etcdClient.Create(MapreduceTaskStatusPath(t.config.AppName, t.taskID, "workStatus"), "non", 0)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
			return nil
		}
		return err
	}
	return nil
}

func (t *workerTask) datarequestForWork(ctx context.Context, method string) {
	t.logger.Println("data request")
	master := t.framework.GetTopology()["Master"].GetNeighbors(t.epoch)

	for _, node := range master {
		t.framework.DataRequest(ctx, node, method, &pb.WorkRequest{TaskID: t.taskID})
	}
}

func (t *workerTask) grabWork(ctx context.Context, method string, stop chan bool) {
	t.logger.Println("in grab work")
	t.datarequestForWork(ctx, method)

	// afterwards, watch etcd worker attribute "workStatus"
	// if exist "set' operation, and the value is "non"
	receiver := make(chan *etcd.Response, 1)
	t.logger.Println(MapreduceTaskStatusPath(t.config.AppName, t.taskID, "workStatus"))
	go t.etcdClient.Watch(MapreduceTaskStatusPath(t.config.AppName, t.taskID, "workStatus"), 0, true, receiver, stop)
	for resp := range receiver {
		if resp.Action != "set" {
			continue
		}
		if resp.Node.Value == "non" {
			t.datarequestForWork(ctx, method)
		} else {
			continue
		}
	}
}

func (t *workerTask) getWorkID() string {
	requestWorkStatus, err := t.etcdClient.Get(MapreduceTaskStatusPath(t.config.AppName, t.taskID, "workStatus"), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get worker status from etcd")
	}
	return requestWorkStatus.Node.Value
}

func (t *workerTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &mapreduceEvent{ctx: ctx, epoch: epoch}
}

func (t *workerTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	// stop the last epoch grab work procedure
	// start a new one
	close(t.stopGrabTaskForEveryEpoch)
	t.stopGrabTaskForEveryEpoch = make(chan bool, 1)
	go t.grabWork(ctx, "/proto.Master/GetWork", t.stopGrabTaskForEveryEpoch)
}

func (t *workerTask) Exit() {
	close(t.stopGrabTaskForEveryEpoch)
	close(t.exitChan)
}

func (t *workerTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterWorkerServer(server, t)
	return server

}

func (t *workerTask) CreateOutputMessage(method string) proto.Message {
	switch method {
	case "/proto.Master/GetWork":
		return new(pb.WorkConfigResponse)
	}
	panic("")
}

func (t *workerTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.dataReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, method: method, output: output}
}

func (t *workerTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	t.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}
