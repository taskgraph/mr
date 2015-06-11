package server

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	pb "github.com/plutoshe/mr/proto"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 10000, "The server port")
	tp   = flag.String("type", "", "The server type, m(mapper)/r(reducer)")
	s    *grpc.Server
)

type server struct{}

func (*server) GetCollectResult(KvPair *pb.ReducerRequest, stream pb.Reducer_GetCollectResultServer) error {
	fmt.Println("===in Collect Function")
	if KvPair.Key == "Stop" && len(KvPair.Value) == 0 {
		// server.Stop()
		s.Stop()
		fmt.Println("Stop")
		return nil
	}
	count := 0
	for i := range KvPair.Value {
		v, err := strconv.Atoi(KvPair.Value[i])
		if err == nil {
			count += v
		}
	}
	stream.Send(&pb.ReducerResponse{Key: KvPair.Key, Value: strconv.Itoa(count)})

	return nil
}

func (*server) GetEmitResult(KvPair *pb.MapperRequest, stream pb.Mapper_GetEmitResultServer) error {
	fmt.Println("===in Emit Function")
	if KvPair.Value == "Stop" && KvPair.Key == "Stop" {
		// server.Stop()
		s.Stop()
		fmt.Println("Stop")
		return nil
	}
	chop := strings.Split(KvPair.Key, " ")
	for i := range chop {
		res := &pb.MapperResponse{
			Key:   chop[i],
			Value: "1",
		}
		// fmt.Println(chop[i])
		if err := stream.Send(res); err != nil {
			return err
		}
	}
	return nil
}

func Main() {
	flag.Parse()
	if *tp == "" {
		log.Fatalln("Need a server type m/r")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Println("Listening...")

	if *tp == "m" {
		s = grpc.NewServer()
		pb.RegisterMapperServer(s, &server{})
		s.Serve(lis)
	} else {
		s = grpc.NewServer()
		pb.RegisterReducerServer(s, &server{})
		s.Serve(lis)
	}
}
