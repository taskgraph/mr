package benchmark

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"strings"

	pb "github.com/plutoshe/mr/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 10000, "The server port")
	tp   = flag.String("type", "", "The server type, m(mapper)/r(reducer)")
	s    *grpc.Server
)

type mapperEmitKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type server struct{}

func (*server) GetEmitResult(KvPair *pb.MapperRequest, stream pb.Mapper_GetEmitResultServer) error {
	if KvPair.Value == "Stop" && KvPair.Key == "Stop" {
		s.Stop()
		fmt.Println("Stop")
		return nil
	}
	chop := strings.Split(KvPair.Key, " ")
	for _, s := range chop {
		res := &pb.MapperResponse{
			Key:   s,
			Value: "1",
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
	return nil
}

func emit(key, val string) {
	h := fnv.New32a()
	h.Write([]byte(key))
	var KV mapperEmitKV
	KV.Key = key
	KV.Value = val
	// toShuffle := h.Sum32() % uint32(writeNum)
	data, err := json.Marshal(KV)
	data = append(data, '\n')
	if err != nil {
		log.Fatalf("json marshal error : ", err)
	}
	// writeCloser[toShuffle].Write(data)
}

func grpcEmitKvPairs(userClient pb.MapperClient, str string, value string, stop bool) {
	stream, err := userClient.GetEmitResult(context.Background(), &pb.MapperRequest{Key: str, Value: value})
	if err != nil {
		log.Fatalf("could not access the user program server : %v", err)
	}
	if !stop {
		for {
			feature, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("%v.GetEmitResult, %v", userClient, err)
				return
			}
			emit(feature.Key, feature.Value)
		}
	}
}

func generalEmitKvPairs(str string, value string) {
	chop := strings.Split(str, " ")
	for _, s := range chop {
		emit(s, "1")
	}

}

func getNewMapperUserServer(address string) pb.MapperClient {
	conn, err := grpc.Dial(address)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewMapperClient(conn)
}

func newMapperSever(address string) (string, func()) {
	flag.Parse()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// fmt.Println("Listening...")
	s = grpc.NewServer()
	pb.RegisterMapperServer(s, &server{})
	go s.Serve(lis)
	return lis.Addr().String(), func() {
		s.Stop()
	}
}
