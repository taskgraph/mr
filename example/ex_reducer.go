	reducerWorkDir := make([]mapreduce.Work, 0)
	for i := 1; i <= *reducerNum; i++ {
		newWork := mapreduce.Work{}
		inputFile := "mapreducerprocesstemporaryresult"
		newWork.InputFilePath = []string{inputFile}

		newWork.OutputFilePath = []string{outputDir}

		newWork.UserProgram = "go run sample_reducer_server.go"
		newWork.UserServerAddress = ""
		newWork.WorkType = "Reducer"
		newWokr.SupplyContent = strconv.Itoa(*mapperNum) + " " + strconv.Itoa(*reducerNum)
		reducerWorkDir = append(reducerWorkDir, newWork)
	}
	reducerConfig := mapreduce.MapreduceConfig{
		MapperNum:  uint64(*mapperNum),
		ReducerNum: uint64(*reducerNum),
		WorkNum:    uint64(*reducerNum),

		AppName:          *job,
		etcdURLs:         etcdURLs,
		FilesystemClient: azureClient,
		WorkDir:          reducerWorkDir,
	}

	