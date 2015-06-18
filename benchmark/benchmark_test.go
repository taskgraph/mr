package benchmark

import (
	"bufio"
	"io"
	"log"
	"testing"

	"github.com/plutoshe/taskgraph/filesystem"
)

var writeCloser []bufio.Writer
var writeNum int

func benchmarkForGeneral(inputFile string, outputNum int) {
	f := filesystem.NewLocalFSClient()
	fin, _ := f.OpenReadCloser(inputFile)
	newReadCloser := *bufio.NewReader(fin)

	// writeCloser = make([]bufio.Writer, 0)
	// writeNum = outputNum

	// for i := 0; i < writeNum; i++ {
	// 	path := "result" + strconv.Itoa(i)
	// 	tmpWrite, err := f.OpenWriteCloser(path)
	// 	if err != nil {
	// 		log.Fatalf("Get writer failed, ", err)
	// 	}
	// 	writeCloser = append(writeCloser, *bufio.NewWriterSize(tmpWrite, 4096))
	// }

	var err error
	var str []byte
	for err != io.EOF {
		str, err = newReadCloser.ReadBytes('\n')
		if err != io.EOF && err != nil {
			log.Fatalf("read Error, ", err)
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		generalEmitKvPairs(string(str), "")
	}
	for i := 0; i < writeNum; i++ {
		writeCloser[i].Flush()
	}
}

func benchmarkForGRPC(inputFile string, outputNum int) {
	f := filesystem.NewLocalFSClient()
	fin, _ := f.OpenReadCloser(inputFile)
	newReadCloser := *bufio.NewReader(fin)

	// newMapperSever(10001)
	// grpcServer := getNewMapperUserServer("localhost:10001")
	// writeNum = outputNum
	// writeCloser = make([]bufio.Writer, 0)

	// for i := 0; i < writeNum; i++ {
	// 	path := "result" + strconv.Itoa(i)
	// 	tmpWrite, err := f.OpenWriteCloser(path)
	// 	if err != nil {
	// 		log.Fatalf("Get writer failed, ", err)
	// 	}
	// 	writeCloser = append(writeCloser, *bufio.NewWriterSize(tmpWrite, 4096))
	// }
	target, stopper := newMapperSever("localhost:0")
	defer stopper()
	grpcServer := getNewMapperUserServer(target)

	var err error
	var str []byte
	for err != io.EOF {
		str, err = newReadCloser.ReadBytes('\n')
		if err != io.EOF && err != nil {
			log.Fatalf("read Error, ", err)
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		grpcEmitKvPairs(grpcServer, string(str), "", false)
	}
	grpcEmitKvPairs(grpcServer, "Stop", "Stop", true)
	for i := 0; i < writeNum; i++ {
		writeCloser[i].Flush()
	}
}

func BenchmarkForGenearl(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkForGeneral("pagesNew001.txt", 1)
	}
}

func BenchmarkForGRPC(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkForGRPC("pagesNew001.txt", 1)
	}
}
