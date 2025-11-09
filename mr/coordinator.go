package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 处理Worker发送来的rpc请求,并分配任务
// 管理任务的状态
// 检测任务是否超时

// Task 任务结构体
type Task struct {
	TaskID int
	FileName string
	TaskStatus string
	StartTime time.Time
}

// Coordinator 协调器结构体
type Coordinator struct {
	// Your definitions here.
	Mutex sync.Mutex
	MapTasks []Task
	ReduceTasks []Task
	MapTasksNum int
	MapFinsh bool
	ReduceFinsh bool
	FileNames []string
	NextTaskID int
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

// AskforTask 获取任务
func (c *Coordinator) AskforTask(args *TaskRequest, reply *TaskRequestReply) error {
	c.Mutex.Lock()	// 加锁
	defer c.Mutex.Unlock()	// 解锁
	if !c.MapFinsh {			// 如果map任务未完成,分发map任务
		ID := c.NextTaskID							// 获取任务ID
		if ID >= c.MapTasksNum {                	// 如果已经遍历完所有任务,则返回-1
    		reply.TaskID = -1
			c.NextTaskID = 0
    		return nil
		}
		for ID < c.MapTasksNum && c.MapTasks[ID].TaskStatus != Idle {
			ID++									// 如果当前任务非空闲,则寻找下一个任务
			if ID >= c.MapTasksNum {				// 如果已经遍历完所有任务,则返回-1
				reply.TaskID = -1
				c.NextTaskID = 0
				return nil
			}
		}
		reply.TaskID = ID							// 设置任务ID
		reply.TaskType = MapTask					// 设置任务类型
		reply.Filename = c.MapTasks[ID].FileName	// 设置任务文件名
		reply.NReduce = c.NReduce					// 设置任务reduce数量
		c.MapTasks[ID].StartTime = time.Now()		// 记录任务开始时间
		c.MapTasks[ID].TaskStatus = Running			// 设置任务状态为running
		c.NextTaskID = ID + 1
		// fmt.Printf("[worker] %d get %s task: %d\n", args.WorkerID, reply.TaskType, ID)
	} else if !c.ReduceFinsh {	// 如果map任务已完成,reduce任务未完成分发reduce任务
		ID := c.NextTaskID							// 获取任务ID
		if ID >= c.NReduce {                		// 如果已经遍历完所有任务,则返回-1
    		reply.TaskID = -1
    		return nil
		}
		for ID < c.NReduce && c.ReduceTasks[ID].TaskStatus != Idle {
			ID++									// 如果当前任务非空闲,则寻找下一个任务
			if ID >= c.NReduce {
				reply.TaskID = -1
				return nil
			}
		}
		reply.TaskID = ID							// 设置任务ID
		reply.TaskType = ReduceTask					// 设置任务类型
		reply.MapTaskNum = c.MapTasksNum			// 设置map任务数量
		reply.ReduceTaskNum  = ID					// 设置reduce任务编号
		reply.NReduce = c.NReduce					// 设置reduce任务数量
		c.ReduceTasks[ID].StartTime = time.Now()	// 设置任务开始时间
		c.ReduceTasks[ID].TaskStatus = Running		// 设置任务状态为running
		c.NextTaskID = ID + 1
		// fmt.Printf("[worker] %d get %s task: %d\n", args.WorkerID, reply.TaskType, ID)
	} else {
		reply.TaskID = -2
	}
	return nil
}

// ReportTask 任务状态报告
func (c *Coordinator) ReportTask(args *TaskReport, reply *TaskReportReply) error {
	c.Mutex.Lock()	// 加锁
	defer c.Mutex.Unlock()	// 解锁
	if args.TaskType == MapTask {
		if args.IsCompleted {
			c.MapTasks[args.TaskID].TaskStatus = Completed
		}
	} else if args.TaskType == ReduceTask {
		if args.IsCompleted {
			c.ReduceTasks[args.TaskID].TaskStatus = Completed
		}
	}
	reply.OK = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//

// CheckTimeout 检查running中的任务是否超时,顺便检查任务是否完成
func (c *Coordinator) CheckTimeout() {
	for{
		time.Sleep(2 *time.Second)	// 每两秒钟检查一次

		if c.ReduceFinsh {			//reduce任务完成,直接退出
			break
		}

		c.Mutex.Lock()				// 锁住

		// 检查map任务
		if !c.MapFinsh {			// map任务未完成
			MapTasksDone := true
			findTimeout := false
			fmt.Printf("MapTask ")
			for id, task := range c.MapTasks {
				fmt.Printf("%d:%s ", id, task.TaskStatus)
				if task.TaskStatus == Running {
					if time.Since(task.StartTime) > 10 * time.Second {		// 如果任务超时
						if findTimeout  {
							continue
						}
						c.MapTasks[id].TaskStatus = Idle					// 设置任务为idel
						c.NextTaskID = id									// 设置下一个任务ID
						findTimeout = true
						fmt.Printf("Map task %d timeout, reset to idel\n", id)
					}
				}
				if task.TaskStatus != Completed {							// 如果任务没有完成
					MapTasksDone = false
				}
			}
			fmt.Printf("\n")
			if MapTasksDone {
				c.MapFinsh = true											// 设置Map任务完成
				c.NextTaskID = 0											// 重置下一个任务ID
			}
		} else if !c.ReduceFinsh {	// map任务完成,reduce任务未完成
			ReduceTasksDone := true
			findTimeout := false
			fmt.Printf("ReduceTask ")
			for id, task := range c.ReduceTasks {
				fmt.Printf("%d:%s ", id, task.TaskStatus)
				if task.TaskStatus == Running {
					if time.Since(task.StartTime) > 10 * time.Second {		// 如果任务超时
						if findTimeout  {
							continue
						}
						c.ReduceTasks[id].TaskStatus = Idle					// 设置任务为idel
						c.NextTaskID = id									// 设置下一个任务ID
						findTimeout = true
						fmt.Printf("Reduce task %d timeout, reset to idel\n", id)
					}
				}
				if task.TaskStatus != Completed {							// 如果任务没有完成
					ReduceTasksDone = false
				}
			}
			fmt.Printf("\n")
			if ReduceTasksDone {
				c.ReduceFinsh = true
			}
		}
		c.Mutex.Unlock()	// 解锁
	}
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	go c.CheckTimeout()	// 启动超时检查
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) Done() bool {
    ret := false
    
    if c.ReduceFinsh {
        ret = true
    }
    return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Mutex = sync.Mutex{}
	c.FileNames = files
	c.NReduce = nReduce
	c.MapFinsh = false
	c.ReduceFinsh = false
	c.NextTaskID = 0
	c.MapTasksNum = len(files)
	c.MapTasks = make([]Task, len(files))
	c.ReduceTasks = make([]Task, nReduce)
	// fmt.Printf("c is %v\n", c)

	// 创建Map任务
	for filenum, filename := range files {
		c.MapTasks[filenum] = Task{
			FileName: filename,
			TaskID:   filenum,
			TaskStatus: Idle,
		}
	}

	// 创建Reduce任务
	for reduceNum := 0; reduceNum < nReduce; reduceNum++ {
		c.ReduceTasks[reduceNum] = Task{
			TaskID:   reduceNum,
			TaskStatus: Idle,
		}
	}

	c.server()
	return &c
}
