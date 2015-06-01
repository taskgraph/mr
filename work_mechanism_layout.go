package mapreduce

// supplement the etcdutil layout.go
// work mechanism layout

import (
	"path"
	"strconv"
)


const (
	MapreduceNodeStatusDir   = "nodeStatus"
)



func MasterPath(job string) string {
	return path.Join("/", job, "master/0")
}
func WorkerPath(job string, id uint64) string {
	return path.Join("/", job, strconv.FormatUint(id, 10))
}

// etcd API for mapreduce based on old framework API
func MapreduceNodeStatusDir(appName string, id uint64) string {
	return path.Join("/", appName, MapreduceWorkerStatusDir, strconv.FormatUint(id, 10))
}

func MapreduceNodeStatusPath(appName string, id uint64, attr) string {
	return path.Join(MapreduceWorkerStatusDir(appName, id), attr)
}
