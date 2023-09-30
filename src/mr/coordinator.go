package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Status string

const (
	STATUS_IDLE Status = "idle"
	STATUS_BUSY Status = "busy"
	STATUS_DONE Status = "done"
	STATUS_FAIL Status = "fail"
)

type Coordinator struct {
	lock sync.Mutex
	Jobs []Job
	Outs []Out
}

type Job struct {
	file    string
	status  Status
	nReduce int
}

type Out struct {
	file   string
	status Status
}

func (c *Coordinator) Job(args *JobArgs, reply *JobReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, j := range c.Jobs {
		if j.status == STATUS_IDLE {
			c.Jobs[i].status = STATUS_BUSY
			reply.WorkerType = WORKER_TYPE_MAP
			reply.Id = i
			reply.File = j.file
			reply.NReduce = j.nReduce
			return nil
		}
	}
	for _, j := range c.Jobs {
		if j.status != STATUS_DONE {
			return fmt.Errorf("map job on going")
		}
	}
	for i, o := range c.Outs {
		if o.status == STATUS_IDLE {
			c.Outs[i].status = STATUS_BUSY
			reply.WorkerType = WORKER_TYPE_REDUCE
			reply.Id = i
			reply.File = o.file
			return nil
		}
	}
	return fmt.Errorf("no job available")
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	if args.WorkerType == WORKER_TYPE_MAP {
		for i, j := range c.Jobs {
			if j.file == args.File {
				c.Jobs[i].status = STATUS_DONE
				return nil
			}
		}
	} else {
		for i, o := range c.Outs {
			if o.file == args.File {
				c.Outs[i].status = STATUS_DONE
				return nil
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	for _, j := range c.Jobs {
		if j.status != STATUS_DONE {
			return false
		}
	}
	for _, o := range c.Outs {
		if o.status != STATUS_DONE {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	jobs := []Job{}
	for _, file := range files {
		jobs = append(jobs, Job{
			file:    file,
			status:  STATUS_IDLE,
			nReduce: nReduce,
		})
	}
	outs := make([]Out, nReduce)
	for i := 0; i < nReduce; i++ {
		outs[i] = Out{
			file:   fmt.Sprintf("mr-out-%d", i),
			status: STATUS_IDLE,
		}
	}
	c := Coordinator{
		Jobs: jobs,
		Outs: outs,
	}

	c.server()
	return &c
}
