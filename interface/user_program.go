package mapreduce

import (
	"os/exec"
	"strings"
	"log"
	"io/ioutil"

	pb "../proto"
	"google.golang.org/grpc"
)

func (t *workerTask) getNewMapperUserServer(address string) pb.MapperStreamClient {
	t.logger.Println(address)
	conn, err := grpc.Dial(address)
	if err != nil {
		t.logger.Fatalf("did not connect: %v", err)
	}
	return pb.NewMapperStreamClient(conn)
}

func (t *workerTask) getNewReducerUserServer(address string) pb.ReducerStreamClient {
	//  + fmt.Sprintf(":%d", t.userSeverPort)
	conn, err := grpc.Dial(address)
	if err != nil {
		t.logger.Fatalf("did not connect: %v", err)
	}
	return pb.NewReducerStreamClient(conn)
}

func (t *workerTask) startNewUserServer(cmdline []string) {
	// output, err := exec.Command("echo $PATH").Output()
	// log.Println(string(output), err)
	// argv := []string{"-port", strconv.FormatUint(t.userSeverPort, 10)}
	for i := 0; i < len(cmdline); i++ {
		parts := strings.Fields(cmdline[i])
		background := parts[0]
		head := parts[1]
		parts = parts[2:len(parts)]
		cmd := exec.Command(head, parts...)
		t.logger.Println(head, parts)
		if background == "b" {
			err := cmd.Start()
			log.Println("background", head, parts, err)
			if err != nil {
				t.logger.Fatalln("background ", head, parts, err)
			}
		} else if background == "w" {
			stdout, _ := cmd.StdoutPipe()
			err := cmd.Start()
			if err != nil {
				t.logger.Fatalln("wait", head, parts, err)
			}
			d, _ := ioutil.ReadAll(stdout)
			err = cmd.Wait()
			log.Println(head, parts, string(d))
			log.Println("run", head, parts, err)
			if err != nil {
				t.logger.Fatalln(err)
			}
		} else {
			err := cmd.Run()
			if err != nil {
				t.logger.Fatalln("run", head, parts, err)
			}
		}
	}

}
