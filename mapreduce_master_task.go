package mapreduce

import (
	"log"
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

const delim = "##"

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
	getWork       chan uint64
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
	t.getWork = make(chan uint64, 1)
	t.dataReady = make(chan *mapreduceEvent, t.config.NodeNum)
	t.metaReady = make(chan *mapreduceEvent, t.config.NodeNum)
	t.notifyChanArr = make([]chan WorkConfig, t.config.NodeNum)
	for i := uint64(0); i < t.config.NodeNum; i++ {
		notifyChan := make(chan WorkConfig, 1)
		t.notifyChanArr = append(t.notifyChanArr, notifyChan)
	}
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
	_, err := t.etcdClient.Create(MapreduceTaskStatusPath(t.config.AppName, 0, "currentWorkNum"), "0", 0)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
		} else {
			return err
		}
	}

	_, err = t.etcdClient.Create(MapreduceTaskStatusPath(t.config.AppName, 0, "workNum"), strconv.Itoa(len(t.config.WorkDir)), 0)
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
			t.logger.Println("meta ready", metaReady)
			go t.processMessage(metaReady.ctx, metaReady.fromID, metaReady.linkType, metaReady.meta)
		case <-t.exitChan:

			return

		}
	}
}

// grpc interface providing worker invoke to grab new work
// implements as a serialize program by channel.
func (t *masterTask) GetWork(in *pb.WorkRequest, stream pb.Master_GetWorkServer) error {
	t.logger.Println("master : receive the request of task ", in.TaskID)
	t.getWork <- in.TaskID
	for {
		select {
		case workConfig := <-t.notifyChanArr[in.TaskID]:
			t.logger.Printf("master : task %d get the work config\n", in.TaskID)
			key := []string{"InputFilePath", "OutputFilePath", "UserServerAddress", "UserProgram", "WorkType", "SupplyContent"}
			val := []string{
				strings.Join(workConfig.InputFilePath, delim),
				strings.Join(workConfig.OutputFilePath, delim),
				workConfig.UserServerAddress,
				strings.Join(workConfig.UserProgram, delim),
				workConfig.WorkType,
				strings.Join(workConfig.SupplyContent, delim),
			}
			if err := stream.Send(&pb.WorkConfigResponse{Key: key, Value: val}); err != nil {
				t.logger.Fatal(err)
				return err
			}
			return nil
		case <-t.workDone:
			return nil
		}
	}
	return nil
}

func (t *masterTask) updateNodeStatus() {

	request, err := t.etcdClient.Get(MapreduceTaskStatusPath(t.config.AppName, 0, "currentWorkNum"), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get master status from etcd")
	}
	t.currentWorkNum, _ = strconv.ParseUint(request.Node.Value, 10, 64)

	request, err = t.etcdClient.Get(MapreduceTaskStatusPath(t.config.AppName, 0, "workNum"), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get master status from etcd")
	}
	t.totalWork, _ = strconv.ParseUint(request.Node.Value, 10, 64)

}

func (t *masterTask) assignWork(taskID uint64) {
	t.logger.Println("master : in assign work procedure for task ", taskID)
	for {

		// check worker work status
		requestWorkStatus, err := t.etcdClient.Get(MapreduceTaskStatusPath(t.config.AppName, taskID, "workStatus"), false, false)
		if err != nil {
			log.Fatal("etcdutil: can not get worker status from etcd")
		}
		if requestWorkStatus.Node.Value != "non" {
			t.logger.Printf("master : task %d already possesses work %s, recovering... \n", taskID, requestWorkStatus.Node.Value)
			workID, _ := strconv.Atoi(requestWorkStatus.Node.Value)
			t.notifyChanArr[taskID] <- t.config.WorkDir[workID]
			return
		}

		// update master overall work state
		t.updateNodeStatus()
		t.logger.Println("workNum", t.currentWorkNum, "totalWork", t.totalWork)
		if t.currentWorkNum >= t.totalWork {
			close(t.workDone)
			return
		}
		t.logger.Printf("master : currentWork %d, totalWork %d, %s\n", t.currentWorkNum, t.totalWork, requestWorkStatus.Node.Value)
		// try grab work by compareAndSwap op
		// if sucessed, transfre work config to pointed task.
		_, err = t.etcdClient.CompareAndSwap(
			MapreduceTaskStatusPath(t.config.AppName, 0, "currentWorkNum"),
			strconv.FormatUint(t.currentWorkNum+1, 10),
			0,
			strconv.FormatUint(t.currentWorkNum, 10),
			0,
		)
		if err != nil {
			if strings.Contains(err.Error(), "Compare failed") {
			} else {
				log.Fatal(err)
			}
		} else {
			t.logger.Printf("master : task %d possesses work %d, starting... \n", taskID, t.currentWorkNum)
			t.etcdClient.Set(MapreduceTaskStatusPath(t.config.AppName, taskID, "workStatus"), strconv.FormatUint(t.currentWorkNum, 10), 0)

			t.notifyChanArr[taskID] <- t.config.WorkDir[t.currentWorkNum]
			return
		}

	}
}

func (t *masterTask) processMessage(ctx context.Context, fromID uint64, linkType string, meta string) {
	t.logger.Println(fromID, meta)
	matchWork, _ := regexp.MatchString("^WorkFinished[0-9]+$", meta)
	switch {
	case matchWork:
		t.finishedWorkNum++
		_, err := t.etcdClient.Set(MapreduceTaskStatusPath(t.config.AppName, fromID, "workStatus"), "non", 0)
		t.logger.Println(meta)
		if err != nil {
			t.logger.Fatalf("Set work status failed")
		}
		t.logger.Println(t.totalWork, t.finishedWorkNum)
		if t.finishedWorkNum >= t.totalWork {
			t.logger.Println("in Exit")
			t.Exit()
			return
		}
	}
}

func (t *masterTask) Exit() {
	t.framework.ShutdownJob()
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
