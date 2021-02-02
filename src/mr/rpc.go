package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetMapTaskReq struct {}

type GetMapTaskResp struct {
	TaskIdx int
	T *MapTask
	NReduce int
}

type GetReduceTaskReq struct {}

type GetReduceTaskResp struct {
	TaskIdx int
	T *ReduceTask
	CanStart bool
}

type DoneMapTaskReq struct {
	TaskDoneIdx int
	IntermediateFiles []string
}

type DoneMapTaskResp struct {
	Success bool
}

type DoneReduceTaskReq struct {
	TaskDoneIdx int
}

type DoneReduceTaskResp struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
