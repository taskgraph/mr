package benchmark

import (
	"encoding/json"
	"hash/fnv"
	"io"
	"log"
	"net"
	"strings"

	pb "./proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var s *grpc.Server

type mapperEmitKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type server struct{}

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

func (*server) GetEmitResult(KvPair *pb.MapperRequest, stream pb.Mapper_GetEmitResultServer) error {
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

func newMapperServer(address string) (string, func()) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v %s", err, address)
	}
	s = grpc.NewServer()
	pb.RegisterMapperServer(s, &server{})
	go s.Serve(lis)
	return lis.Addr().String(), func() {
		s.Stop()
	}
}

func (s *server) GetStreamEmitResult(stream pb.MapperStream_GetStreamEmitResultServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		chop := strings.Split(in.Key, " ")
		for _, s := range chop {
			res := &pb.MapperResponse{
				Key:   s,
				Value: "1",
			}
			if err := stream.Send(res); err != nil {
				return err
			}
		}
	}
}

func getNewMapperStreamUserServer(address string) pb.MapperStreamClient {
	conn, err := grpc.Dial(address)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewMapperStreamClient(conn)
}

func newMapperStreamServer(address string) (string, func()) {

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v %s", err, address)
	}
	s = grpc.NewServer()
	pb.RegisterMapperStreamServer(s, &server{})
	go s.Serve(lis)
	return lis.Addr().String(), func() {
		s.Stop()
	}

}
