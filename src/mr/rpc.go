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

const (
	Success int = 0
	WaitReduce int = 1
	WaitExit int = 2
	Exit    int = -1
)

type TaskType int64

type Task struct {
	TaskType TaskType
	TaskId int64
	FileName string
}

type FetchTaskArgs struct{
	WorkId int64
}

type FetchTaskReply struct{
	WorkId   int64
	NReduce  int
	NMap     int
	Code     int
	Task     *Task
}

type CommitTaskArgs struct{
	Task   *Task
	WorkId int64
}

type CommitTaskReply struct{
	Code int
	TempFiles []string
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
