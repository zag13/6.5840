package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		if err != nil {
			if err.Error() == ErrJobNotReady.Error() {
				time.Sleep(100 * time.Millisecond)
			} else {
				isMore = false
			}
		}

		switch reply.WorkerType {
		case WORKER_TYPE_MAP:
			err := mapWorker(mapf, reply.File, reply.Id, reply.NReduce)
			if err != nil {
				log.Fatal("map worker failed: ", err)
			}
		case WORKER_TYPE_REDUCE:
			err := reduceWorker(reducef, reply.File, reply.Id)
			if err != nil {
				log.Fatal("reduce worker failed: ", err)
			}
		}
	}
}

func mapWorker(mapf func(string, string) []KeyValue, filename string, id, nReduce int) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()

	files := make([]*os.File, nReduce)
	for i, _ := range files {
		name := fmt.Sprintf("mr-%d-%d", id, i)
		file, _ = os.Create(name)
		files[i] = file
	}

	kva := mapf(filename, string(content))
	for _, kv := range kva {
		enc := json.NewEncoder(files[ihash(kv.Key)%nReduce])
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}

	for _, file := range files {
		file.Close()
	}

	if err := CallReport(WORKER_TYPE_MAP, filename); err != nil {
		return err
	}
	return nil
}

func reduceWorker(reducef func(string, []string) string, filename string, id int) error {
	files, err := os.ReadDir("./")
	if err != nil {
		return err
	}
	mfiles := []string{}
	re := regexp.MustCompile(fmt.Sprintf("mr-\\d-%d", id))
	for _, file := range files {
		if !file.IsDir() && re.MatchString(file.Name()) {
			mfiles = append(mfiles, file.Name())
		}
	}

	kva := []KeyValue{}
	for _, mfile := range mfiles {
		file, err := os.Open(mfile)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	ofile, _ := os.Create(filename)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	if err := CallReport(WORKER_TYPE_REDUCE, filename); err != nil {
		return err
	}
	return nil
}

func CallJob() (JobReply, error) {
	args, reply := JobArgs{}, JobReply{}

	err := call("Coordinator.Job", &args, &reply)

	return reply, err
}

func CallReport(workerType WorkerType, file string) error {
	args := ReportArgs{
		WorkerType: workerType,
		File:       file,
	}
	reply := ReportReply{}

	return call("Coordinator.Report", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns nil.
// returns error if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
