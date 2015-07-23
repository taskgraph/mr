package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	pb "../proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	address     = "192.168.59.103"
	defaultName = "world"
)

var (
	port = flag.Int("port", 20000, "The server port")
	// c    pb.MapperClient
)

func main() {
	// Set up a connection to the server.
	flag.Parse()
	fmt.Println("link server ", (address + fmt.Sprintf(":%d", *port)))
	conn, err := grpc.Dial(address + fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	grpcServer := pb.NewReducerStreamClient(conn)

	// Contact the server and print out its response.
	fmt.Println("link server ", (address + fmt.Sprintf(":%d", *port)))

	waitc := make(chan struct{})
	stream, err := grpcServer.GetStreamCollectResult(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			in, err := stream.Recv()
			log.Println(in)
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to receive a note : %v", err)
				return
			}
			if err == io.EOF || (len(in.Arr) == 1 && in.Arr[0].Key == "Stop" && in.Arr[0].Value == "Stop") {

				close(waitc)
				return
			}
		}
	}()
	var b []*pb.KvsPair
	b = append(b, &pb.KvsPair{"a b c d e 。f g，", []string{"1", "2", "3"}})
	b = append(b, &pb.KvsPair{"a b c d e 。f g，", []string{"1", "2", "3"}})
	b = append(b, &pb.KvsPair{"a b c d e 。f g，", []string{"1", "2", "3"}})
	b = append(b, &pb.KvsPair{"a b c d e 。f g，", []string{"1", "2", "3"}})

	stream.Send(&pb.ReducerRequest{b})
	stream.Send(&pb.ReducerRequest{b})
	b = nil
	b = append(b, &pb.KvsPair{"a a c dsdf。 e f gww", []string{""}})
	stream.Send(&pb.ReducerRequest{b})

	b = append(b, &pb.KvsPair{"a a c dsdf e， f ｀！gww", []string{""}})
	// stream.Send(&pb.ReducerRequest{b})
	// stream.Send(&pb.ReducerRequest{})
	stream.CloseSend()
	<-waitc

}
