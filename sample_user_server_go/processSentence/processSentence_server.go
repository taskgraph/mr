package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"unicode/utf8"

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
		defer s.Stop()
		fmt.Println("Stop")
		return nil
	}
	stream.Send(&pb.ReducerResponse{Key: KvPair.Key, Value: ""})

	return nil
}

func (*server) GetStreamCollectResult(stream pb.Reducer_GetStreamCollectResultServer) error {

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			defer s.Stop()
			return nil
		}
		if err != nil {
			return err
		}
		str = in.Key
		err = stream.Send(&pb.MapperResponse{Key: str, Value: ""})
		if err != nil {
			return err
		}

	}

	return nil
}

func (*server) GetStreamEmitResult(stream pb.MapperStream_GetStreamEmitResultServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			defer s.Stop()
			return nil
		}
		if err != nil {
			return err
		}
		// if in.Value == "Stop" && in.Key == "Stop" {
		// 	// server.Stop()
		// 	res := &pb.MapperResponse{
		// 		Key:   "Stop",
		// 		Value: "Stop",
		// 	}
		// 	if err := stream.Send(res); err != nil {
		// 		return err
		// 	}
		// 	defer s.Stop()
		// 	return nil
		// }

		str := in.Key
		chop := ""
		str += "。"

		for len(str) > 0 {
			cc, size := utf8.DecodeRuneInString(str)
			if cc == '，' || cc == '。' || cc == '？' || cc == '！' || cc == '；' {
				if len(chop) >= 3 && len(chop) <= 30 {
					res := &pb.MapperResponse{
						Key:   chop,
						Value: "1",
					}
					// fmt.Println(chop[i])
					err := stream.Send(res)
					if err != nil {
						return err
					}
				}
				chop = ""
			} else {
				chop += fmt.Sprintf("%c", cc)
			}
			str = str[size:]
		}
	}
}

func (*server) GetEmitResult(KvPair *pb.MapperRequest, stream pb.Mapper_GetEmitResultServer) error {
	fmt.Println("===in Emit Function")

	if KvPair.Value == "Stop" && KvPair.Key == "Stop" {
		// server.Stop()
		defer s.Stop()
		fmt.Println("Stop")
		return nil
	}

	str := KvPair.Key
	chop := ""
	str += "。"

	for len(str) > 0 {
		cc, size := utf8.DecodeRuneInString(str)
		log.Println(cc)
		if cc == '，' || cc == '。' || cc == '？' || cc == '！' || cc == '；' {
			if len(chop) >= 3 && len(chop) <= 30 {
				res := &pb.MapperResponse{
					Key:   chop,
					Value: "1",
				}
				// fmt.Println(chop[i])
				err = stream.Send(res)
				if err != nil {
					return err
				}
			}
			chop = ""
		} else {
			chop += fmt.Sprintf("%c", cc)
		}
		str = str[size:]
	}

	return nil
}

func main() {
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
		pb.RegisterMapperStreamServer(s, &server{})
		s.Serve(lis)
	} else {
		s = grpc.NewServer()
		pb.RegisterReducerServer(s, &server{})
		s.Serve(lis)
	}
}
