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
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	mapreduce "github.com/plutoshe/mr/interface"
	"github.com/plutoshe/taskgraph/controller"
	"github.com/plutoshe/taskgraph/example/topo"
	"github.com/plutoshe/taskgraph/filesystem"
	"github.com/plutoshe/taskgraph/framework"
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
	ETCDURL       string
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
	DockerPort    string
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
	logDir         = flag.String("logdir", "./logdir", "the output log file")
	phase          = flag.String("phase", "i", "The phase of application")
)

func fsInit() {
	if config.FSType == "Azure" {
		blobServiceBaseUrl := "core.chinacloudapi.cn"
		apiVersion := "2014-02-14"
		userHttps := false
		fsClient, err = filesystem.NewAzureClient(
			config.AzureConfig.AzureAccount,
			config.AzureConfig.AzureKey,
			blobServiceBaseUrl,
			apiVersion,
			userHttps,
		)
		if err != nil {
			log.Fatalln("MapReduce : get mapreduce filesystem client writer failed, ", err)
		}
		// tmpWrite, _ := fsClient.OpenWriteCloser("tmptest0123456789012345678901234/aaa")
		// tmpWrite.Write([]byte("受到法律框架酸辣粉谁，是否。\nsdf wedfsdf. . ， 。d.f.s！\n地方！ dfg。 sdf。 sdf。 sdf， sdf，"))
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
			"wc sudo docker stop mr" + strconv.Itoa(i),
			"wc sudo docker rm mr" + strconv.Itoa(i),
			"ww sudo docker run -d -p " +
				strconv.Itoa(20000+i) +
				":" +
				config.DockerPort +
				" --name=mr" +
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
			"wc sudo docker stop mr" + strconv.FormatUint(i, 10),
			"wc sudo docker rm mr" + strconv.FormatUint(i, 10),
			"ww sudo docker run -d -p " +
				strconv.FormatUint(i+20000, 10) +
				":" +
				config.DockerPort +
				" --name=mr" +
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

func clean(client *etcd.Client) {
	client.Delete(config.AppName, true)
}

func mapperTaskInit() {
	etcdURLs := []string{config.ETCDURL}
	if *phase == "i" {
		clean(etcd.NewClient(etcdURLs))
	}
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
	etcdURLs := []string{config.ETCDURL}
	if *phase == "i" {
		clean(etcd.NewClient(etcdURLs))
	}
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
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	log.Println(taskConfig)
	log.Println(taskConfig.AppName)
	log.Println(taskConfig.EtcdURLs)
	switch programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(taskConfig.AppName, etcd.NewClient(taskConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()
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

func mapperTaskConfig(phase string) {
	mapperTaskInit()
	switch phase {
	case "c", "t":
		taskExec(phase, mapperConfig)
	case "i":
		log.Println("in dispatching")

		subCom := strings.Fields("run example.go -phase c -source " + *sourceConfig)
		controllerProcess := exec.Command("go", subCom...)

		if *logDir != "" {
			os.Mkdir(*logDir, os.ModePerm)
			controllerLog, err := os.Create(*logDir + "/controller.log")
			if err != nil {
				log.Fatalln(err)
			}
			controllerProcess.Stdout = controllerLog
			controllerProcess.Stderr = controllerLog
		}

		err = controllerProcess.Start()
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(2000 * time.Millisecond)
		for i := uint64(0); i < 1+config.WorkerNum+config.Tolerance; i++ {
			subCom := strings.Fields("run example.go -phase t -source " + *sourceConfig)
			taski := exec.Command("go", subCom...)
			if *logDir != "" {
				taskiLog, err := os.Create(*logDir + "/task" + strconv.FormatUint(i, 10) + ".log")
				if err != nil {
					log.Fatalln(err)
				}
				taski.Stdout = taskiLog
				taski.Stderr = taskiLog
			}

			taski.Start()
		}
	}

}

func reducerTaskConfig(phase string) {
	reducerTaskInit()
	switch phase {
	case "c", "t":
		taskExec(phase, reducerConfig)
	case "i":
		log.Println("in dispatching")

		subCom := strings.Fields("run example.go -phase c -source " + *sourceConfig)
		controllerProcess := exec.Command("go", subCom...)

		if *logDir != "" {
			os.Mkdir(*logDir, os.ModePerm)
			controllerLog, err := os.Create(*logDir + "/controller.log")
			if err != nil {
				log.Fatalln(err)
			}
			controllerProcess.Stdout = controllerLog
			controllerProcess.Stderr = controllerLog
		}

		err = controllerProcess.Start()
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(2000 * time.Millisecond)
		for i := uint64(0); i < 1+config.WorkerNum+config.Tolerance; i++ {
			subCom := strings.Fields("run example.go -phase t -source " + *sourceConfig)
			taski := exec.Command("go", subCom...)
			if *logDir != "" {
				taskiLog, err := os.Create(*logDir + "/task" + strconv.FormatUint(i, 10) + ".log")
				if err != nil {
					log.Fatalln(err)
				}
				taski.Stdout = taskiLog
				taski.Stderr = taskiLog
			}

			taski.Start()
		}
	}
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
	if *phase != "c" && *phase != "i" && *phase != "t" {
		log.Fatalf("Please specify a phase(i/c/t) \n i : a initialize phase, \n c : a conctorller pahse, \n t : an offspring pahse.")
	}
	file, _ := os.Open(*sourceConfig)
	defer file.Close()
	decoder := json.NewDecoder(file)

	config = Configuration{}

	err := decoder.Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(config)
	if config.Type == "Mapper" {
		mapperTaskConfig(*phase)
	} else if config.Type == "Reducer" {
		reducerTaskConfig(*phase)
	} else {
		log.Println("Pleas specify the application type in configuration file")
	}

}
