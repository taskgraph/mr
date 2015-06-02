package mapreduce

import (
	"github.com/golang/protobuf/proto"
	"github.com/plutoshe/taskgraph/filesystem"
	"golang.org/x/net/context"
)

// This structure implements a setup of mapreduce,
// decribes the work mechanism, the app name,
// etcd url, the buffer size, IO filesystem.

type MapreduceConfig struct {
	//defined the work num
	MapperNum  uint64
	ShuffleNum uint64
	ReducerNum uint64
	WorkNum    uint64
	NodeNum    uint64

	//store the work, appname, and etcdurls
	FilesystemClient filesystem.Client
	WorkDir          []WorkConfig
	AppName          string
	EtcdURLs         []string

	//optional, define the buffer size
	ReaderBufferSize int
	WriterBufferSize int
}

// This structure decribes concrete setting of a work,
// including cmdline for user program,
// workType, and input/output file path.

// TO-DO :
// user code shoule implement as single structure

type WorkConfig struct {
	InputFilePath     []string
	OutputFilePath    []string
	UserProgram       []string
	WorkType          string
	UserServerAddress string
	SupplyContent     []string
}

type mapreduceEvent struct {
	ctx      context.Context
	epoch    uint64
	fromID   uint64
	workID   uint64
	linkType string
	method   string
	meta     string
	output   proto.Message
}
