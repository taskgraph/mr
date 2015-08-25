package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strconv"

	mapreduce "../../mr/interface"
	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph/controller"
	"github.com/plutoshe/taskgraph/example/topo"
	"github.com/plutoshe/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph/framework"
)

// Input files defined in "input($mapperTaskID).txt"
func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) redu er")
	job := flag.String("job", "mapreduce+reducer", "job name")
	mapperNum := flag.Int("mapperNum", 5, "mapperNum")
	WorkerNum := flag.Int("WorkerNum", 3, "WorkerNum")
	reducerNum := flag.Int("reducerNum", 10, "reducerNum")
	// azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "", "azureAccountKey")
	// outputDir := flag.String("outputDir", "0newmapreducepathformapreduce000", "outputDir")

	flag.Parse()
	if *job == "" {
		log.Fatalf("Please specify a job name")
	}
	if *azureAccountKey == "" {
		log.Fatalf("Please specify azureAccountKey")
	}

	azureClient := filesystem.NewLocalFSClient()

	reducerWorkDir := make([]mapreduce.WorkConfig, 0)
	for i := 0; i < *reducerNum; i++ {
		newWork := mapreduce.WorkConfig{}
		inputFile := "/home/xwu/Desktop/processSentence/mr/example/mapreducerprocesstemporaryresult"
		newWork.InputFilePath = []string{inputFile}
		newWork.OutputFilePath = []string{"./reducerOutput" + strconv.Itoa(i)}

		newWork.UserProgram = []string{
			"b ../sample_user_server_go/processSentence/processSentence_server -type r -port " + strconv.Itoa(40000+i),
		}
		newWork.UserServerAddress = "localhost:" + strconv.Itoa(40000+i)
		newWork.WorkType = "Reducer"
		newWork.SupplyContent = []string{strconv.Itoa(*mapperNum) + " " + strconv.Itoa(i)}
		reducerWorkDir = append(reducerWorkDir, newWork)
	}

	var ll *log.Logger
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	etcdURLs := []string{"http://localhost:4001"}

	reducerConfig := mapreduce.MapreduceConfig{
		MapperNum:  uint64(*mapperNum),
		ReducerNum: uint64(*reducerNum),
		WorkNum:    uint64(*reducerNum),
		WorkerNum:  uint64(*WorkerNum),

		AppName:          *job,
		EtcdURLs:         etcdURLs,
		FilesystemClient: azureClient,
		WorkDir:          reducerWorkDir,
	}

	ntask := uint64(*WorkerNum) + 1
	topoMaster := topo.NewFullTopologyOfMaster(uint64(*WorkerNum) + 1)
	topoNeighbors := topo.NewFullTopologyOfNeighbor(uint64(*WorkerNum) + 1)

	switch *programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(reducerConfig.AppName, etcd.NewClient(reducerConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()

	case "t":
		log.Printf("reducer task")
		bootstrap := framework.NewBootStrap(reducerConfig.AppName, reducerConfig.EtcdURLs, createListener(), ll)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{MapreduceConfig: reducerConfig}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.AddLinkage("Master", topoMaster)
		bootstrap.AddLinkage("Neighbors", topoNeighbors)
		bootstrap.Start()
	default:
		log.Fatal("Please choose a type: (c) controller, (t) task")
	}
}

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
