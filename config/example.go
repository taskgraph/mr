package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph/controller"
	"github.com/plutoshe/taskgraph/example/topo"
	"github.com/plutoshe/taskgraph/filesystem"
	"github.com/plutoshe/taskgraph/framework"

	mapreduce "../../mr/interface"
)

type AzureFsConfiguration struct {
	AzureAccount       string
	AzureKey           string
	BlobServiceBaseUrl string
	ApiVersion         string
	UseHttps           bool
}

type Configuration struct {
	Type          string
	ETCDBIN       string
	AppName       string
	FSType        string
	AzureConfig   AzureFsConfiguration
	OutputDir     string
	InputFiles    []string
	MapperWorkNum uint64
	WorkerNum     uint64
	ReducerNum    uint64
	Tolerance     uint64
	TmpResultDir  string
	DockerImage   string
	DockerIp      string
}

var (
	fsClient       filesystem.Client
	mapperWorkDir  []mapreduce.WorkConfig
	reducerWorkDir []mapreduce.WorkConfig
	mapperConfig   mapreduce.MapreduceConfig
	reducerConfig  mapreduce.MapreduceConfig
	config         Configuration
	err            error
	finshedProgram chan struct{}
	sourceConfig   = flag.String("source", "", "The configuration file")
)

func fsInit() {
	if config.FSType == "Azure" {
		blobServiceBaseUrl := "core.chinacloudapi.cn"
		apiVersion := "2014-02-14"
		userHttps := true
		fsClient, err = filesystem.NewAzureClient(
			config.AzureConfig.AzureAccount,
			config.AzureConfig.AzureKey,
			blobServiceBaseUrl,
			apiVersion,
			userHttps,
		)

	}
	if config.FSType == "Local" {
		fsClient = filesystem.NewLocalFSClient()

	}
}

func mapperWorkInit() {
	mapperWorkDir = make([]mapreduce.WorkConfig, 0)
	for i, inputFile := range config.InputFiles {
		newWork := mapreduce.WorkConfig{}
		newWork.InputFilePath = []string{inputFile}
		newWork.OutputFilePath = []string{config.TmpResultDir}
		newWork.UserProgram = []string{
			"wc docker stop mr" + strconv.Itoa(i),
			"wc docker rm mr" + strconv.Itoa(i),
			"ww docker run -d -p " +
				strconv.Itoa(20000+i) +
				":10000 --name=mr" +
				strconv.Itoa(i) +
				" " +
				config.DockerImage,
		}

		newWork.UserServerAddress = config.DockerIp + ":" + strconv.Itoa(20000+i)
		newWork.WorkType = "Mapper"
		newWork.SupplyContent = []string{""}
		mapperWorkDir = append(mapperWorkDir, newWork)
	}
}

func reducerWorkInit() {
	reducerWorkDir = make([]mapreduce.WorkConfig, 0)
	for i := uint64(0); i < config.ReducerNum; i++ {
		newWork := mapreduce.WorkConfig{}
		inputFile := config.TmpResultDir
		newWork.InputFilePath = []string{inputFile}
		newWork.OutputFilePath = []string{config.OutputDir + "/reducerOutput" + strconv.FormatUint(i, 10)}

		newWork.UserProgram = []string{
			"wc docker stop mr" + strconv.FormatUint(i, 10),
			"wc docker rm mr" + strconv.FormatUint(i, 10),
			"ww docker run -d -p " +
				strconv.FormatUint(i+20000, 10) +
				":10000 --name=mr" +
				strconv.FormatUint(i, 10) +
				" " +
				config.DockerImage,
		}
		newWork.UserServerAddress = config.DockerIp + ":" + strconv.FormatUint(i+20000, 10)
		newWork.WorkType = "Reducer"
		newWork.SupplyContent = []string{strconv.FormatUint(config.MapperWorkNum, 10) + " " + strconv.FormatUint(i, 10)}
		reducerWorkDir = append(reducerWorkDir, newWork)
	}
}

func clean() {
	cmd := exec.Command(config.ETCDBIN+"/etcdctl", "rm", "--recursive", config.AppName+"/")
	cmd.Run()
}

func mapperTaskInit() {
	etcdURLs := []string{"http://localhost:4001"}
	clean()
	fsInit()
	mapperWorkInit()

	mapperConfig = mapreduce.MapreduceConfig{
		ReducerNum: config.ReducerNum,
		WorkerNum:  config.WorkerNum,

		AppName:          config.AppName,
		EtcdURLs:         etcdURLs,
		FilesystemClient: fsClient,
		WorkDir:          mapperWorkDir,
	}
}

func reducerTaskInit() {
	etcdURLs := []string{"http://localhost:4001"}
	clean()
	fsInit()
	reducerWorkInit()

	reducerConfig = mapreduce.MapreduceConfig{
		ReducerNum: config.ReducerNum,
		WorkerNum:  config.WorkerNum,

		AppName:          config.AppName,
		EtcdURLs:         etcdURLs,
		FilesystemClient: fsClient,
		WorkDir:          reducerWorkDir,
	}
}

func taskExec(programType string, taskConfig mapreduce.MapreduceConfig) {
	ntask := uint64(config.WorkerNum) + 1
	topoMaster := topo.NewFullTopologyOfMaster(uint64(config.WorkerNum) + 1)
	topoNeighbors := topo.NewFullTopologyOfNeighbor(uint64(config.WorkerNum) + 1)

	var ll *log.Logger
	// fmt.Println("logFilename=", logFilename)
	// logFile, err := os.OpenFile(logFilename, os.O_RDWR|os.O_CREATE, 0777)
	// if err != nil {
	// 	fmt.Printf("open file error=%s\r\n", err.Error())
	// 	os.Exit(-1)
	// }
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	switch programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(taskConfig.AppName, etcd.NewClient(taskConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()
		close(finshedProgram)
	case "t":
		log.Printf("mapper task")
		bootstrap := framework.NewBootStrap(taskConfig.AppName, taskConfig.EtcdURLs, createListener(), ll)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{MapreduceConfig: taskConfig}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.AddLinkage("Master", topoMaster)
		bootstrap.AddLinkage("Neighbors", topoNeighbors)
		bootstrap.Start()
	default:
		log.Fatal("Please choose a type: (c) controller, (t) task")
	}
}

func mapperTaskConfig() {
	mapperTaskInit()
	finshedProgram = make(chan struct{}, 1)
	go taskExec("c", mapperConfig)
	time.Sleep(2000 * time.Millisecond)
	for i := uint64(0); i < 1+config.WorkerNum+config.Tolerance; i++ {
		go taskExec("t", mapperConfig)
	}

	<-finshedProgram

}

func reducerTaskConfig() {
	reducerTaskInit()
	finshedProgram = make(chan struct{}, 1)
	go taskExec("c", reducerConfig)
	time.Sleep(2000 * time.Millisecond)
	for i := uint64(0); i < 1+config.WorkerNum+config.Tolerance; i++ {
		go taskExec("t", reducerConfig)
	}

	<-finshedProgram

}

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}

func main() {
	flag.Parse()
	if *sourceConfig == "" {
		log.Fatalf("Please specify a configuration file")
	}
	file, _ := os.Open(*sourceConfig)
	decoder := json.NewDecoder(file)

	config = Configuration{}

	err := decoder.Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(config)
	if config.Type == "Mapper" {
		mapperTaskConfig()
	} else if config.Type == "Reducer" {
		reducerTaskConfig()
	} else {
		log.Println("Pleas specify the application type in configuration file")
	}

}
