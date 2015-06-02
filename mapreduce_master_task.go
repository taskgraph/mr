package mapreduce

import (
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"

	pb "./proto"
	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const nonExistWork = math.MaxUint64

type masterTask struct {
	framework       taskgraph.Framework
	taskType        string
	epoch           uint64
	logger          *log.Logger
	taskID          uint64
	numOfTasks      uint64
	etcdClient      *etcd.Client
	currentWorkNum  uint64
	totalWork       uint64
	finishedWorkNum uint64

	//channels
	epochChange   chan *mapreduceEvent
	dataReady     chan *mapreduceEvent
	metaReady     chan *mapreduceEvent
	getWork       chan int
	finishedChan  chan *mapreduceEvent
	notifyChanArr []chan WorkConfig
	exitChan      chan struct{}
	workDone      chan bool

	config MapreduceConfig
}

func (t *masterTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.etcdClient = etcd.NewClient(t.config.EtcdURLs)
	t.epochChange = make(chan *mapreduceEvent, 1)
	t.getWork = make(chan int, 1)
	t.dataReady = make(chan *mapreduceEvent, t.config.NodeNum)
	t.metaReady = make(chan *mapreduceEvent, t.config.NodeNum)
	t.notifyChanArr = make([]chan WorkConfig, t.config.NodeNum)
	t.finishedWorkNum = 0
	t.workDone = make(chan bool, 1)
	// for i := range t.notifyChanArr {
	// 	t.notifyChanArr[i] = make(bool, 1)

	// }
	t.exitChan = make(chan struct{})
	err := t.initializeEtcd()
	if err != nil {
		t.logger.Fatal(err)
	}
	go t.run()
}

func (t *masterTask) initializeEtcd() error {
	_, err := t.etcdClient.Create(MapreduceNodeStatusPath(t.config.AppName, 0, "currentWorkNum"), "0", 0)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
		} else {
			return err
		}
	}

	_, err = t.etcdClient.Create(MapreduceNodeStatusPath(t.config.AppName, 0, "workNum"), strconv.Itoa(len(t.config.WorkDir)), 0)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
		} else {
			return err
		}
	}
	return nil

}

func (t *masterTask) run() {
	for {
		select {
		case requestWorker := <-t.getWork:
			t.assignWork(requestWorker)
		case metaReady := <-t.metaReady:
			go t.processMessage(metaReady.ctx, metaReady.fromID, metaReady.linkType, metaReady.meta)
		case <-t.exitChan:
			return

		}
	}
}

// grpc interface providing worker invoke to grab new work
// implements as a serialize program by channel.
func (t *masterTask) GetWork(in *pb.WorkRequest, stream pb.Master_GetWorkServer) error {
	requestTaskID, _ := strconv.Atoi(in.TaskID)
	t.getWork <- requestTaskID
	for {
		select {
		case workConfig := <-t.notifyChanArr[requestTaskID]:
			key := []string{"InputFilePath", "OutputFilePath", "UserServerAddress", "UserProgram", "WorkType", "SupplyContent"}
			val := []string{
				workConfig.InputFilePath,
				workConfig.OutputFilePath,
				workConfig.UserServerAddress,
				workConfig.UserProgram,
				workConfig.WorkType,
				workConfig.SupplyContent,
			}
			stream.Send(&pb.WorkConfigResponse{Key: key, Value: val})
			return nil
		case <-t.workDone:
			return nil
		}
	}
	return nil
}

func (t *masterTask) updateNodeStatus() {

	request, err := t.etcdClient.Get(MapreduceNodeStatusPath(t.config.AppName, 0, "currentWorkNum"), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get master status from etcd")
	}
	t.currentWorkNum, _ = strconv.ParseUint(request.Node.Value, 10, 64)

	request, err = t.etcdClient.Get(MapreduceNodeStatusPath(t.config.AppName, 0, "workNum"), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get master status from etcd")
	}
	t.totalWork, _ = strconv.ParseUint(request.Node.Value, 10, 64)

}

func (t *masterTask) assignWork(taskID int) {

	for {

		// check worker work status
		requestWorkStatus, err := t.etcdClient.Get(MapreduceNodeStatusPath(t.config.AppName, taskID, "workStatus"), false, false)
		if err != nil {
			log.Fatal("etcdutil: can not get worker status from etcd")
		}
		if requestWorkStatus.Node.Value != "non" {
			workID, _ := strconv.Atoi(requestWorkStatus.Node.Value)

			t.notifyChanArr[taskID] <- t.config.WorkDir[workID]
			return
		}

		// update master overall work state
		t.updateNodeStatus()

		if t.currentWorkNum >= t.totalWork {
			close(t.workDone)
			return
		}

		// try grab work by compareAndSwap op
		// if sucessed, transfre work config to pointed task.
		grabWork, err := t.etcdClient.CompareAndSwap(
			MapreduceNodeStatusPath(t.config.AppName, 0, "currentWorkNum"),
			strconv.FormatUint(t.currentWorkNum+1, 10),
			0,
			strconv.FormatUint(t.currentWorkNum, 10),
			0,
		)
		if err != nil {
			log.Fatal(err)
		}

		if grabWork {
			t.notifyChanArr[taskID] <- t.config.WorkDir[t.currentWorkNum]
			return
		}
	}
}

func (t *masterTask) processMessage(ctx context.Context, fromID uint64, linkType string, meta string) {
	matchWork, _ := regexp.MatchString("^WorkFinished[0-9]+$", meta)
	switch {
	case matchWork:
		t.finishedWorkNum++
		setWorkStatus, err := t.etcdClient.Set(MapreduceNodeStatusPath(t.config.AppName, fromID, "workStatus"), "non", 0)
		if err != nil {
			t.logger.Fatalf("Set work status failed")
		}

		if t.finishedWorkNum >= t.totalWork {
			t.Exit()
			return
		}
	}
}

func (t *masterTask) Exit() {
	close(t.exitChan)
}

func (t *masterTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	t.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: linkType, meta: meta}
}

func (*masterTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {

}

func (t *masterTask) EnterEpoch(ctx context.Context, epoch uint64) {

}

func (t *masterTask) CreateOutputMessage(method string) proto.Message {
	switch method {
	case "/proto.Master/GetWork":
		return new(pb.WorkConfigResponse)
	}
	panic("")
}

func (t *masterTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterMasterServer(server, t)
	return server
}
