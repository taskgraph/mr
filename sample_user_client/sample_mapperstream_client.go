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
	address     = "localhost"//"192.168.59.103" //"192.168.59.103"
	defaultName = "world"
)

var (
	port = flag.Int("port", 10000, "The server port")
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

	grpcServer := pb.NewMapperStreamClient(conn)

	// Contact the server and print out its response.
	fmt.Println("link server ", (address + fmt.Sprintf(":%d", *port)))

	waitc := make(chan struct{})
	stream, err := grpcServer.GetStreamEmitResult(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to receive a note : %v", err)
				return
			}
			if err == io.EOF || (len(in.Arr) == 1 && in.Arr[0].Key == "Stop" && in.Arr[0].Value == "Stop") {
				// read done.
				close(waitc)
				return
			}

			fmt.Println(in)
		}
	}()

	var b []*pb.KvPair
	b = append(b, &pb.KvPair{"a b c d e 。f g，", ""})
	b = append(b, &pb.KvPair{"a b c d e 。f g，", ""})
	b = append(b, &pb.KvPair{"a b c d e 。f g，", ""})

	stream.Send(&pb.MapperRequest{b})
	stream.Send(&pb.MapperRequest{b})
	b = nil
	b = append(b, &pb.KvPair{"a a c dsdf e， f ｀！gww", ""})
	stream.Send(&pb.MapperRequest{b})

	b = nil
	b = append(b, &pb.KvPair{"aaaaaaaaaaaaaaaaaaaaaaaaaaaa", ""})
	stream.Send(&pb.MapperRequest{b})

	stream.Send(&pb.MapperRequest{})
 stream.CloseSend()
	<-waitc

}
