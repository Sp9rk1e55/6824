package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type OpType int

type TaskState int

type ReplyArgs struct {
	RepID      int64
	RepOp      OpType
	RepTaskId  int
	RepnMap    int
	RepnReduce int
	RepContent string
}

type ReqArgs struct {
	ReqID     int64
	ReqOp     OpType
	ReqTaskId int
}

const (
	TaskReq OpType = iota
	TaskMap
	TaskReduce
	TaskMapDone
	TaskReduceDone
	TaskDone
	TaskWait
)

const (
	Running TaskState = iota
	Stopped
	Rebooting
	Terminated
)

// Add your RPC definitions here.

type Rpc struct {
	args  ReqArgs
	reply ReplyArgs
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
