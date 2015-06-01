package mapreduce

import "github.com/taskgraph/taskgraph"

type MapreduceTaskBuilder struct {
	NumOfTasks      uint64
	MapreduceConfig MapreduceConfig
}

func (tb MapreduceTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	if taskID == 0 {
		return &masterTask{
			config: tb.MapreduceConfig,
		}
	} else {
		return &workerTask{
			config: tb.MapreduceConfig,
		}
	}
}
