package main

import (
	"flag"
	"fmt"
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
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "mapreduce+mapper", "job name")
	mapperNum := flag.Int("mapperNum", 10, "mapperNum")
	WorkerNum := flag.Int("WorkerNum", 4, "WorkerNum")
	reducerNum := flag.Int("reducerNum", 10, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "", "azureAccountKey")
	// outputDir := flag.String("outputDir", "0newmapreducepathformapreduce000", "outputDir")

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

	mapperWorkDir := make([]mapreduce.WorkConfig, 0)

	for inputM := 1; inputM <= *mapperNum; inputM++ {
		w := fmt.Sprintf("%03d", 2+10)
		inputFile := "00000pagestestmapreduceframework/pagesNew" + w + ".txt"
		newWork := mapreduce.WorkConfig{}
		newWork.InputFilePath = []string{inputFile}
		newWork.OutputFilePath = []string{"mapreducerprocesstemporaryresult"}
		// newWork.UserProgram = []string{
		// 	"docker stop mr" + strconv.Itoa(inputM),
		// 	"docker rm mr" + strconv.Itoa(inputM),
		// 	"docker run -d -p " + strconv.Itoa(20000+inputM) + ":10000 --name mr" + strconv.Itoa(inputM) + " plutoshe/mr:mr-new go run main.go -type m",
		// }
		newWork.UserProgram = []string{
			"b ../sample_user_server_go/processSentence/processSentence_server -type m -port " + strconv.Itoa(20000+inputM),
		}
		//../sample_mapper_user_program/sample_mapper_server
		// 192.168.59.103
		newWork.UserServerAddress = "localhost:" + strconv.Itoa(20000+inputM)
		newWork.WorkType = "Mapper"
		newWork.SupplyContent = []string{""}
		mapperWorkDir = append(mapperWorkDir, newWork)
	}

	var ll *log.Logger
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	etcdURLs := []string{"http://localhost:4001"}

	mapperConfig := mapreduce.MapreduceConfig{
		MapperNum:  uint64(*mapperNum),
		ReducerNum: uint64(*reducerNum),
		WorkNum:    uint64(*mapperNum),
		WorkerNum:  uint64(*WorkerNum),

		AppName:          *job,
		EtcdURLs:         etcdURLs,
		FilesystemClient: azureClient,
		WorkDir:          mapperWorkDir,
	}

	ntask := uint64(*WorkerNum) + 1
	topoMaster := topo.NewFullTopologyOfMaster(uint64(*WorkerNum) + 1)
	topoNeighbors := topo.NewFullTopologyOfNeighbor(uint64(*WorkerNum) + 1)

	switch *programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(mapperConfig.AppName, etcd.NewClient(mapperConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()

	case "t":
		log.Printf("mapper task")
		bootstrap := framework.NewBootStrap(mapperConfig.AppName, mapperConfig.EtcdURLs, createListener(), ll)
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
