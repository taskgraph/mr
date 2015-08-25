package main

import (
	"encoding/json"
	"fmt"
	"os"
)

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

type AzureFsConfiguration struct {
	AzureAccount       string
	AzureKey           string
	BlobServiceBaseUrl string
	ApiVersion         string
	UseHttps           bool
}

func mapperSetting() Configuration {
	var mapperConfig Configuration
	mapperConfig.Type = "Mapper"
	mapperConfig.ETCDURL = "http://localhost:4001"
	mapperConfig.AppName = "mapperEx"

	mapperConfig.FSType = "Azure"
	mapperConfig.AzureConfig.AzureAccount = "spluto"
	mapperConfig.AzureConfig.AzureKey = "q85g4/FJrxGzcc8JAyqryw48P9WNKKkPApVrfXnm2uPWjkmRx/uH21mFgBlSok93bq06NT/iZKMsfF9MNp8a7Q=="

	mapperConfig.InputFiles = make([]string, 0)
	for i := 1; i <= 10; i++ {
		st := fmt.Sprintf("%03d.txt", i)
		mapperConfig.InputFiles = append(mapperConfig.InputFiles, "pagesnewdatastorageonlyforlength/pagesNew"+st) //"pagesNew"+strconv.Itoa(i)+".txt")
	}

	mapperConfig.ReducerNum = 10
	mapperConfig.WorkerNum = 10
	mapperConfig.Tolerance = 3

	mapperConfig.TmpResultDir = "mapreducerprocesstemporaryresult"
	//"mapreducerprocesstemporaryresult"

	mapperConfig.DockerIp = "192.168.59.103"
	mapperConfig.DockerImage = "plutoshe/dockerhubautobuild:mapper"
	mapperConfig.DockerPort = "10000"

	return mapperConfig
}

func reducerSetting() Configuration {
	var reducerConfig Configuration
	reducerConfig.Type = "Reducer"
	reducerConfig.ETCDURL = "http://localhost:4001"
	reducerConfig.AppName = "reducerEx"

	reducerConfig.FSType = "Azure"
	reducerConfig.AzureConfig.AzureAccount = "spluto"
	reducerConfig.AzureConfig.AzureKey = "q85g4/FJrxGzcc8JAyqryw48P9WNKKkPApVrfXnm2uPWjkmRx/uH21mFgBlSok93bq06NT/iZKMsfF9MNp8a7Q=="

	reducerConfig.MapperWorkNum = 10
	reducerConfig.ReducerNum = 10
	reducerConfig.WorkerNum = 10
	reducerConfig.Tolerance = 3

	reducerConfig.TmpResultDir = "mapreducerprocesstemporaryresult"
	reducerConfig.OutputDir = "mapreduceprocessresultsofreducer"
	reducerConfig.DockerIp = "192.168.59.103"
	reducerConfig.DockerImage = "plutoshe/dockerhubautobuild:reducer"
	reducerConfig.DockerPort = "10000"

	return reducerConfig
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	convertConfig := mapperSetting()
	m, err := json.Marshal(convertConfig)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	convertConfig = reducerSetting()
	r, err := json.Marshal(convertConfig)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	mapperConfigFile, err := os.Create("mappertest.json")
	reducerConfigFile, err := os.Create("reducertest.json")
	defer mapperConfigFile.Close()
	defer reducerConfigFile.Close()
	if err != nil {
		fmt.Println(err)
	} else {
		mapperConfigFile.WriteString(string(m))
		reducerConfigFile.WriteString(string(r))
	}
}
