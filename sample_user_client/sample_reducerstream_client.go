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
			}
			if err == io.EOF || (in.Key == "Stop" && in.Value == "Stop") {

				close(waitc)
				return
			}
		}
	}()

	stream.Send(&pb.ReducerRequest{"a b c d e 。f g，", []string{"1", "2", "3"}})
	stream.Send(&pb.ReducerRequest{"a a c dsdf。 e f gww", []string{""}})
	stream.Send(&pb.ReducerRequest{"a a c dsdf e， f ｀！gww", []string{""}})

}
