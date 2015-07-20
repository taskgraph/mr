package main

import (
	"encoding/json"
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
	ETCDBIN      string
	AppName      string
	FSType       string
	AzureConfig  AzureFsConfiguration
	InputFiles   []string
	MapperNum    uint64
	WorkerNum    uint64
	ReducerNum   uint64
	Tolerance    uint64
	TmpResultDir string
	DockerImage  string
	DockerIp     string
}

var (
	fsClient       filesystem.Client
	mapperWorkDir  []mapreduce.WorkConfig
	mapperConfig   mapreduce.MapreduceConfig
	config         Configuration
	err            error
	finshedProgram chan struct{}
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

func workInit() {
	mapperWorkDir = make([]mapreduce.WorkConfig, 0)
	for inputM, inputFile := range config.InputFiles {
		newWork := mapreduce.WorkConfig{}
		newWork.InputFilePath = []string{inputFile}
		newWork.OutputFilePath = []string{config.TmpResultDir}
		newWork.UserProgram = []string{
			"wc docker stop mr" + strconv.Itoa(inputM),
			"wc docker rm mr" + strconv.Itoa(inputM),
			"ww docker run -d -p " +
				strconv.Itoa(20000+inputM) +
				":10000 --name=mr" +
				strconv.Itoa(inputM) +
				" " +
				config.DockerImage,
		}

		newWork.UserServerAddress = config.DockerIp + ":" + strconv.Itoa(20000+inputM)
		newWork.WorkType = "Mapper"
		newWork.SupplyContent = []string{""}
		mapperWorkDir = append(mapperWorkDir, newWork)
	}
}

func clean() {
	cmd := exec.Command(config.ETCDBIN+"/etcdctl", "rm", "--recursive", config.AppName+"/")
	err := cmd.Run()
	if err != nil {
		log.Fatal("clean failed ", err)
	}
	// err := cmd.Start()
	// if err != nil {
	// 	log.Fatal("clean failed ", err)
	// }
	// err = cmd.Wait()
	// if err != nil {
	// 	log.Fatal("clean failed ", err)
	// }
}

func taskInit() {
	etcdURLs := []string{"http://localhost:4001"}
	clean()
	fsInit()
	workInit()
	if config.MapperNum < config.WorkerNum {
		config.WorkerNum = config.MapperNum
	}
	mapperConfig = mapreduce.MapreduceConfig{
		MapperNum:  config.MapperNum,
		ReducerNum: config.ReducerNum,
		WorkerNum:  config.WorkerNum,

		AppName:          config.AppName,
		EtcdURLs:         etcdURLs,
		FilesystemClient: fsClient,
		WorkDir:          mapperWorkDir,
	}
}

func taskExec(programType string) {
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
		controller := controller.New(mapperConfig.AppName, etcd.NewClient(mapperConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()
		close(finshedProgram)
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

func taskConfig() {
	taskInit()
	finshedProgram = make(chan struct{}, 1)
	go taskExec("c")
	time.Sleep(2000 * time.Millisecond)
	for i := uint64(0); i < 1+config.WorkerNum+config.Tolerance; i++ {
		go taskExec("t")
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
	file, _ := os.Open("mapper.json")
	decoder := json.NewDecoder(file)

	config = Configuration{}

	err := decoder.Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}

	taskConfig()
}
