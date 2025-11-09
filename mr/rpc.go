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

// 任务种类常量
const (
	MapTask = "map"
	ReduceTask = "reduce"
)

// 任务状态常量
const (
	Idle = "idle"
	Running = "running"
	Completed = "completed"
)

// TaskRequest 请求任务
type TaskRequest struct {
	WorkerID int
}

type TaskRequestReply struct {
	TaskID int			// 如果置为-1表示没有任务
	TaskType string
	Filename string
	MapTaskNum int
	ReduceTaskNum int 	// reduce任务的编号,即负责分区的编号
	NReduce int			// reduce任务的总数量
}

// TaskReport 汇报任务状态
type TaskReport struct {
	TaskID int
	TaskType string
	WorkerID int
	IsCompleted bool
}

type TaskReportReply struct {
	OK bool
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
