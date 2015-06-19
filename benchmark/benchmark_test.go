package benchmark

import (
	"bufio"
	"io"
	"log"
	"testing"

	pb "./proto"
	"github.com/plutoshe/taskgraph/filesystem"
	"golang.org/x/net/context"
)

var writeCloser []bufio.Writer
var writeNum int

func benchmarkForGeneral(inputFile string) {
	f := filesystem.NewLocalFSClient()
	fin, _ := f.OpenReadCloser(inputFile)
	newReadCloser := *bufio.NewReader(fin)
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
}

func benchmarkForGRPC(inputFile string) {
	f := filesystem.NewLocalFSClient()
	fin, _ := f.OpenReadCloser(inputFile)
	newReadCloser := *bufio.NewReader(fin)
	target, stopper := newMapperServer("localhost:0")
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
}

func benchmarkForGRPCstream(inputFile string) {
	f := filesystem.NewLocalFSClient()
	fin, _ := f.OpenReadCloser(inputFile)
	newReadCloser := *bufio.NewReader(fin)
	target, stopper := newMapperStreamServer("localhost:0")
	defer stopper()
	grpcServer := getNewMapperStreamUserServer(target)
	// stream := grpcStreamEmitKvPairs(grpcServer)
	// stream = stream
	waitc := make(chan struct{})
	stream, err := grpcServer.GetStreamEmitResult(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive a note : %v", err)
			}
			emit(in.Key, in.Value)
		}
	}()

	// var err error
	var str []byte
	for err != io.EOF {
		str, err = newReadCloser.ReadBytes('\n')
		if err != io.EOF && err != nil {
			log.Fatalf("read Error, ", err)
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		stream.Send(&pb.MapperRequest{Key: string(str), Value: ""})
	}
	stream.CloseSend()
	<-waitc
}

func BenchmarkForGenearl(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkForGeneral("pagesNew001.txt")
	}
}

func BenchmarkForGRPC(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkForGRPC("pagesNew001.txt")
	}
}

func BenchmarkForGRPCstream(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkForGRPCstream("pagesNew001.txt")
	}
}
