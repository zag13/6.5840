package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type WorkerType string

const (
	WORKER_TYPE_MAP    WorkerType = "map"
	WORKER_TYPE_REDUCE WorkerType = "reduce"
)

// Add your RPC definitions here.
type JobArgs struct {
}
type JobReply struct {
	WorkerType WorkerType
	Id         int
	File       string
	NReduce    int
}

type ReportArgs struct {
	WorkerType WorkerType
	File       string
}
type ReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
