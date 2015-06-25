package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	pb "github.com/plutoshe/mr/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	address     = "localhost" //"192.168.59.103"
	defaultName = "world"
)

var (
	port = flag.Int("port", 10000, "The server port")
	c    pb.MapperClient
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
			if (in.Key == "Stop" && in.Value == "Stop") || err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive a note : %v", err)
			}
			fmt.Println(in)
		}
	}()

	stream.Send(&pb.MapperRequest{"a b c d e 。f g，", ""})
	stream.Send(&pb.MapperRequest{"a a c dsdf。 e f gww", ""})
	stream.Send(&pb.MapperRequest{"a a c dsdf e， f ｀！gww", ""})

	stream.Send(&pb.MapperRequest{"Stop", "Stop"})
	// stream.CloseSend()
	<-waitc

}
