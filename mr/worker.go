package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

// ByKey : for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//

// 向coordinator请求任务
// 执行map或者reduce任务
// 处理文件的输入输出
// 汇报任务状态

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerID := os.Getpid()	// 获取进程id

	for {
		tr := TaskRequest{WorkerID: workerID}
		trr := TaskRequestReply{}
		ok := call("Coordinator.AskforTask", &tr, &trr) 
		if !ok {				// 请求任务失败
			fmt.Println("Request task fail")
			time.Sleep(time.Second)
			continue
		}
		if trr.TaskID == -1 {	// 等会再请求
			//fmt.Println("No task")
			time.Sleep(time.Second)
			continue
		} else if trr.TaskID == -2 {
			fmt.Println("All task done")
			break
		}
		// fmt.Printf("Get task %v\n", trr)

		// 请求任务成功,处理任务
		if trr.TaskType == MapTask { 	// 处理map任务
			file, err := os.Open(trr.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", trr.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", trr.Filename)
			}
			file.Close()
			
			// 执行map函数
			kva := mapf(trr.Filename, string(content))

			// 创建nReduce个中间文件
			intermediateFiles := make([]*os.File, trr.NReduce)
			encoders := make([]*json.Encoder, trr.NReduce)

			for i := 0; i < trr.NReduce; i++ {
				filename := "mr-" + strconv.Itoa(trr.TaskID) + "-" + strconv.Itoa(i)
				intermediateFiles[i], err = os.Create(filename)
				if err != nil {
					log.Fatal("create intermediate file error")
				}
				encoders[i] = json.NewEncoder(intermediateFiles[i])
			}

			// 将kv对分配到不同的文件
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % trr.NReduce
				encoders[reduceTaskNum].Encode(&kv)
			}

			// 关闭所有文件
			for i := 0; i < trr.NReduce; i++ {
				intermediateFiles[i].Close()
			}

			// 报告任务完成
			trp := TaskReport{
				IsCompleted: true,
				TaskID: trr.TaskID,
				TaskType: trr.TaskType,
				WorkerID: workerID,
			}
			trpr := TaskReportReply{
				OK: false,
			}
			call("Coordinator.ReportTask", &trp, &trpr)
			if trpr.OK {
				fmt.Printf("Maptask %d done by worker %d\n", trr.TaskID, workerID)
			} else {
				fmt.Printf("Maptask %d report failed\n", trr.TaskID)
			}
		} else {	// 处理reduce任务
			// 收集所有map任务产生的对应reduce文件
			intermediate := []KeyValue{}
			for i := 0; i < trr.MapTaskNum; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(trr.ReduceTaskNum)
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// 排序
			sort.Sort(ByKey(intermediate))

			// 创建输出文件（每个reduce任务一个单独的输出文件）
			oname := "mr-out-" + strconv.Itoa(trr.ReduceTaskNum)
			ofile, _ := os.Create(oname)

			// 执行reduce操作
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// 写入输出文件
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()

			// 报告任务完成
			trp := TaskReport{
				IsCompleted: true,
				TaskID: trr.TaskID,
				TaskType: trr.TaskType,
				WorkerID: workerID,
			}
			trpr := TaskReportReply{
				OK: false,
			}
			call("Coordinator.ReportTask", &trp, &trpr)
			if trpr.OK {
				fmt.Printf("Reducetask %d done by worker %d\n", trr.TaskID, workerID)
			} else {
				fmt.Printf("Reducetask %d report failed\n", trr.TaskID)
			}
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
