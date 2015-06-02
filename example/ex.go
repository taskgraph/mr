package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strconv"

	mapreduce "../../mr"
	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph/example/topo"
	"github.com/plutoshe/taskgraph/filesystem"
	"github.com/plutoshe/taskgraph/framework"
	"github.com/taskgraph/taskgraph/controller"
)

// Input files defined in "input($mapperTaskID).txt"
func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "mapreduce+mapper", "job name")
	mapperNum := flag.Int("mapperNum", 5, "mapperNum")
	reducerNum := flag.Int("reducerNum", 3, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "", "azureAccountKey")
	outputDir := flag.String("outputDir", "0newmapreducepathformapreduce000", "outputDir")

	flag.Parse()
	if *job == "" {
		log.Fatalf("Please specify a job name")
	}
	if *azureAccountKey == "" {
		log.Fatalf("Please specify azureAccountKey")
	}

	azureClient, err := filesystem.NewAzureClient(
		*azureAccountName,
		*azureAccountKey,
		"core.chinacloudapi.cn",
		"2014-02-14",
		true,
	)
	if err != nil {
		log.Fatalf("%v", err)
	}

	mapperWorkDir := make([]mapreduce.Work, 0)

	for inputM := 1; inputM <= *mapperNum; inputM++ {
		inputFile := "testforcomplexmapreduceframework/textForExamination" + strconv.Itoa(inputM)
		newWork := mapreduce.Work{}
		newWork.InputFilePath = []string{inputFile}
		newWork.OutputFilePath = []string{"mapreducerprocesstemporaryresult"}
		newWork.UserProgram = "./sample_mapper_server"
		newWork.UserServerAddress = ""
		newWork.WorkType = "Mapper"
		newWokr.SupplyContent = ""
		mapperWorkDir = append(mapperWorkDir, newWork)
	}

	var ll *log.Logger
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	etcdURLs := []string{"http://localhost:4001"}

	mapperConfig := mapreduce.MapreduceConfig{
		MapperNum:  uint64(*mapperNum),
		ReducerNum: uint64(*reducerNum),
		WorkNum:    uint64(*mapperNum),
		NodeNum : uint64(*mapperNum)

		AppName:          *job,
		EtcdURLs:         etcdURLs,
		FilesystemClient: azureClient,
		WorkDir:          mapperWorkDir,
	}

	ntask := *mapperNum
	topoMaster := topo.NewFullTopologyOfMaster(uint64(*mapperNum) + 1)
	topoNeighbors := topo.NewFullTopologyOfNeighbor(uint64(*mapperNum) + 1)

	switch *programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(mapreduceConfig.AppName, etcd.NewClient(mapreduceConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()

	case "t":
		log.Printf("mapper task")
		bootstrap := framework.NewBootStrap(mapreduceConfig.AppName, mapreduceConfig.EtcdURLs, createListener(), ll)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{MapreduceConfig: mapperConfig}
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
