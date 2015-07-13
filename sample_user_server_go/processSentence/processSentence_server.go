package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"unicode/utf8"

	pb "../../proto"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 10000, "The server port")
	tp   = flag.String("type", "", "The server type, m(mapper)/r(reducer)")
	s    *grpc.Server
)

type server struct{}

func (*server) GetStreamCollectResult(stream pb.ReducerStream_GetStreamCollectResultServer) error {

	for {
		in, err := stream.Recv()
		if len(in.Arr) == 0 {

			err = stream.Send(&pb.ReducerResponse{[]*pb.KvPair{&pb.KvPair{Key: "Stop", Value: "Stop"}}})
			if err != nil {
				return err
			}
			defer s.Stop()
			return nil
		}
		if err != nil {
			return err
		}
		var buf []*pb.KvPair
		for _, v := range in.Arr {
			buf = append(buf, &pb.KvPair{Key: v.Key, Value: ""})

		}
		err = stream.Send(&pb.ReducerResponse{buf})
		if err != nil {
			return err
		}
	}

	return nil
}

// func (*server) GetCollectResult(KvPair *pb.ReducerRequest, stream pb.Reducer_GetCollectResultServer) error {
// 	fmt.Println("===in Collect Function")
// 	if KvPair.Key == "Stop" && len(KvPair.Value) == 0 {
// 		// server.Stop()
// 		defer s.Stop()
// 		fmt.Println("Stop")
// 		return nil
// 	}
// 	stream.Send(&pb.ReducerResponse{Key: KvPair.Key, Value: ""})

// 	return nil
// }

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
		if len(in.Arr) == 0 {
			// server.Stop()
			res := &pb.MapperResponse{
				[]*pb.KvPair{
					&pb.KvPair{
						Key:   "Stop",
						Value: "Stop",
					},
				},
			}
			if err := stream.Send(res); err != nil {
				return err
			}
			// defer s.Stop()
			return nil
		}
		var buf []*pb.KvPair
		for _, v := range in.Arr {
			str := v.Key
			chop := ""
			str += "。"

			for len(str) > 0 {
				cc, size := utf8.DecodeRuneInString(str)
				if cc == '，' || cc == '。' || cc == '？' || cc == '！' || cc == '；' {
					if len(chop) >= 3 && len(chop) <= 30 {
						buf = append(buf, &pb.KvPair{Key: chop, Value: "1"})
						// fmt.Println(chop[i])

					}
					chop = ""
				} else {
					chop += fmt.Sprintf("%c", cc)
				}
				str = str[size:]
			}
		}

		err = stream.Send(&pb.MapperResponse{buf})
		if err != nil {
			return err
		}
	}
}

// func (*server) GetEmitResult(KvPair *pb.MapperRequest, stream pb.Mapper_GetEmitResultServer) error {
// 	fmt.Println("===in Emit Function")

// 	if KvPair.Value == "Stop" && KvPair.Key == "Stop" {
// 		// server.Stop()
// 		defer s.Stop()
// 		fmt.Println("Stop")
// 		return nil
// 	}

// 	str := KvPair.Key
// 	chop := ""
// 	str += "。"

// 	for len(str) > 0 {
// 		cc, size := utf8.DecodeRuneInString(str)
// 		log.Println(cc)
// 		if cc == '，' || cc == '。' || cc == '？' || cc == '！' || cc == '；' {
// 			if len(chop) >= 3 && len(chop) <= 30 {
// 				res := &pb.MapperResponse{
// 					Key:   chop,
// 					Value: "1",
// 				}
// 				// fmt.Println(chop[i])
// 				err := stream.Send(res)
// 				if err != nil {
// 					return err
// 				}
// 			}
// 			chop = ""
// 		} else {
// 			chop += fmt.Sprintf("%c", cc)
// 		}
// 		str = str[size:]
// 	}

// 	return nil
// }

func main() {
	flag.Parse()
	if *tp == "" {
		log.Fatalln("Need a server type m/r")
	}

	lis, err := net.Listen("tcp4", fmt.Sprintf(":%d", *port))
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
		pb.RegisterReducerStreamServer(s, &server{})
		s.Serve(lis)
	}
}
