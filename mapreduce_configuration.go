package mapreduce

import "github.com/plutoshe/taskgraph/filesystem"

// This structure implements a setup of mapreduce,
// decribes the work mechanism, the app name,
// etcd url, the buffer size, IO filesystem.

type MapreduceConfig struct {
	//defined the work num
	WorkNum uint64

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
type WorkConfig struct {
	InputFilePath  string
	OutputFilePath string
	UserProgram    string
	WorkType       string
}
