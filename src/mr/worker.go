package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	isMore := true
	for isMore {
		reply, err := CallJob()
		if err != nil && err.Error() != "map job on going" {
			isMore = false
		}

		switch reply.WorkerType {
		case WORKER_TYPE_MAP:
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()
			kva := mapf(reply.File, string(content))

			files := []*os.File{}
			for i := 0; i < reply.NReduce; i++ {
				name := fmt.Sprintf("mr-%d-%d", reply.Id, i)
				file, _ := os.Create(name)
				files = append(files, file)
			}
			for _, kv := range kva {
				fmt.Fprintf(files[ihash(kv.Key)%reply.NReduce], "%v %v\n", kv.Key, kv.Value)
			}
			for _, file := range files {
				file.Close()
			}

			if err := CallReport(WORKER_TYPE_MAP, reply.File); err != nil {
				log.Fatal("call report failed")
			}
		case WORKER_TYPE_REDUCE:
			re := regexp.MustCompile(`mr-\d-` + fmt.Sprintf("%d", reply.Id) + `$`)
			files, err := os.ReadDir("./")
			if err != nil {
				log.Fatal("read dir failed")
			}
			mfiles := []string{}
			for _, file := range files {
				if !file.IsDir() && re.MatchString(file.Name()) {
					mfiles = append(mfiles, file.Name())
				}
			}

			output := map[string]int{}
			for _, mfile := range mfiles {
				file, err := os.Open(mfile)
				if err != nil {
					log.Fatalf("cannot open %v", mfile)
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := strings.Trim(scanner.Text(), " ")
					if line != "" {
						kv := strings.Split(line, " ")
						output[kv[0]]++
					}
				}
				file.Close()
			}

			ofile, err := os.Create(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}

			for key, val := range output {
				fmt.Fprintf(ofile, "%v %v\n", key, reducef(key, make([]string, val)))
			}
			ofile.Close()

			if err := CallReport(WORKER_TYPE_REDUCE, reply.File); err != nil {
				log.Fatal("call report failed")
			}
		default:
			fmt.Println("unknown worker type")
		}
	}
}

func CallJob() (JobReply, error) {
	args := JobArgs{}
	reply := JobReply{}

	ok := call("Coordinator.Job", &args, &reply)
	if !ok {
		return reply, fmt.Errorf("call failed")
	}

	return reply, nil
}

func CallReport(workerType WorkerType, file string) error {
	args := ReportArgs{
		WorkerType: workerType,
		File:       file,
	}
	reply := ReportReply{}

	ok := call("Coordinator.Report", &args, &reply)
	if !ok {
		return fmt.Errorf("call failed")
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
