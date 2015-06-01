package mapreduce

import (
	"log"
	"math"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph//taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/coreos/go-etcd/etcd"
)

const nonExistWork = math.MaxUint64

type masterTask struct {
	framework  taskgraph.Framework
	taskType   string
	epoch      uint64
	logger     *log.Logger
	taskID     uint64
	numOfTasks uint64
	etcdClient *etcd.Client
	currentWorkNum uint64
	totalWork uint64

	//channels
	epochChange  chan *mapreduceEvent
	dataReady    chan *mapreduceEvent
	metaReady    chan *mapreduceEvent
	getWork		chan int
	finishedChan chan *mapreduceEvent
	notifyChanArr   []chan *WorkConfig
	exitChan     chan struct{}
	workDone 	chan bool

	config MapreduceConfig
}

func (t *masterTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.epochChange = make(chan *event, 1)
	t.getWork = make(chan *event, t.numOfTasks)
	t.dataReady = make(chan *event, t.numOfTasks)
	t.metaReady = make(chan *event, t.numOfTasks)
	t.notifyChanArr = make([]chan *event, t.numOfTasks)
	t.workDone = make(bool, 1)
	for i := range t.notifyChanArr {
		t.notifyChanArr[i] = make(bool, 1)

	}
	t.workerDone = make(chan *event, 1)
	t.exitChan = make(chan *event)
	go t.run()
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
func (t *masterTask) GetWork(in *WorkRequest) (*WorkConfigResponse, error) {
	t.getWork<-in.TaskID
	for {
		select {	
			case workConfig := <-t.notifyChanArr[in.TaskID]:
				key := []string{"InputFilePath", "OutputFilePath", "UserProgram", "WorkType"}
				val := []string{
					workConfig.InputFilePath,
					workConfig.OutputFilePath,
					workConfig.UserProgram,
					workConfig.WorkType,
				}
				return &pb.WorkConfigResponse{Key : key, Value : val}, nil 
			case <-t.workDone :
				return nil, nil
		}		
	}
	return nil, nil
}

func (t *masterTask) assignWork(taskID int) {
	
	for {
		requestWorkStatus, err := t.etcdClient.Get(MapreduceNodeStatusPath(appname, taskID, "workStatus"), false, false)
		if err != nil {
			log.Fatal("etcdutil: can not get worker status from etcd")
		}
		if requestWorkStatus.Node.Value != "non" {
			t.notifyChanArr[taksID] <- t.config.WorkDir[requestWorkStatus]
			return
		}
		t.currentWorkNum, err := t.etcdClient.Get(MapreduceNodeStatusPath(appname, 0, "currentWorkNum"), false, false)
		if err != nil {
			log.Fatal("etcdutil: can not get master status from etcd")
		}
		t.totalWork, err := t.etcdClient.Get(MapreduceNodeStatusPath(appname, 0, "workNum"), false, false)
		if err != nil {
			log.Fatal("etcdutil: can not get master status from etcd")
		}
		if (t.currentWorkNum + 1 >= t.totalWork) {
			close(t.workDone)
			return
		}
		// get workNum through etcd
		// t.currentWorkConfig = // get current Work through etcd
		grabWork, err := t.etcdClient.CompareAndSwap(
			MapreduceNodeStatusPath(appname, 0, "currentWorkNum"), 
			t.currentWorkNum + 1,
			0, 
			t.currentWorkNum, 
			0
		)
		if err != nil {		
			log.Fatal("etcdutil: can not get epoch from etcd")
		}
		if (grabWork) {
			t.notifyChanArr[taskID] <- t.config.WorkDir[t.currentWorkNum+1]//work struture
			temCir = false
		}
	}
}

func (t *masterTask) processMessage(ctx context.Context, fromID uint64, linkType string, meta string) {
	switch t.taskType {
	case "master":
		matchWork, _ := regexp.MatchString("^WorkFinished[0-9]+$", meta)
		switch {
		case matchWork:
			t.finishedWorkNum++
			
			if t.finishedWorkNum >= t.totalWork {
				t.Exit()
				return
			}

	}
}


func (*masterTask) Exit() {
	close(t.exitChan)
}

func (t *masterTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	t.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}

func (*masterTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {

}

func (t *masterTask) initializeEnv() {
	t.workNum = // get workNum through etcd
	t.currentWork = // get current Work through etcd
}

func (t *masterTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.initializeEnv()
}

func (t *bwmfTask) CreateOutputMessage(method string) proto.Message {
	switch method {
	case "/proto.Master/GetWork":
		return new(pb.WorkConfigResponse)
	}
	panic("")
}

func (t *masterTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterBlockDataServer(server, t)
	return server
}
