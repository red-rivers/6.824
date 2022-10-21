package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := 0
	for ;; {
		// fmt.Println("for loop")
		args := &FetchTaskArgs{WorkId: int64(workerId)}
		reply := &FetchTaskReply{}
		success := call("Coordinator.FetchTask", args, reply)
		// log.Printf("FetchWork : %+v", reply)
		// time.Sleep(time.Second * 5)
		// log.Printf("%+v", reply.Task)
		// log.Printf("%+v", reply)
		if workerId == 0 {
			workerId = int(reply.WorkId)
		} 
		if !success  {
			fmt.Println("call Coordinator.FetchTask err")
			return
		}

		// startTime := time.Now()
		// log.Printf("fetch task :%+v, reply: %+v",reply.Task, reply)

		if reply.Code == WaitReduce { 
			// time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
			// log.Println("WAIT")
			time.Sleep(time.Second * 1)
			continue
		}
		// if reply.Code == WaitExit {
		// 	time.Sleep(time.Duration(500) * time.Millisecond)
		// 	// time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
		// 	continue
		// }
		if reply.Code == Exit {
			break
		}
	
		if reply.Task.TaskType == MAP{
			filename := reply.Task.FileName
			file, err := os.Open(reply.Task.FileName)
			if err != nil {
				log.Printf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Printf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			for i:=0; i<reply.NReduce; i++{
				ofile, err := os.Create(tmpMapOutputFile(reply.WorkId, reply.Task.TaskId, i))
				if err != nil {
					log.Printf("open file err : %+v", err)
					return
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range kva{
					if ihash(kv.Key)%reply.NReduce == i{
						err := enc.Encode(&kv)
						if err != nil{
							log.Printf("encode kv : %+v, err : %+b",kv, err)
							return
						}
					}
				}
				ofile.Close()
			}
		}
		if reply.Task.TaskType == REDUCE {
			kva := make([]KeyValue, 0)
			i := 0
			for i<reply.NMap{
				// log.Println(finalMapOutputFile(int64(i), int(reply.Task.TaskId)))
				ofile, err := os.Open(finalMapOutputFile(int64(i), int(reply.Task.TaskId)))
				if err != nil {
					log.Fatalf("open file : %+v, err : %+v",finalMapOutputFile(int64(i), int(reply.Task.TaskId)), err)
					return
				}
				dec := json.NewDecoder(ofile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil{
						break
					}
					kva = append(kva, kv)
				}
				i++
			}
			// fmt.Printf("%s %d\n",finalMapOutputFile(int64(i), int(reply.Task.TaskId)),len(kva))
			ofile, err := os.Create(tmpReduceOutputFile(reply.WorkId, int(reply.Task.TaskId)))
			if err != nil{
				log.Fatalf("open file err : %+v", err)
				return
			}
			sort.Sort(ByKey(kva))
			i = 0
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
				// log.Printf("jobcount reduceF output : %+v", output)
	
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
	
				i = j
			}
			ofile.Close()
		}

		cArgs := CommitTaskArgs{Task: reply.Task, WorkId: int64(workerId)}
		cReply := &CommitTaskReply{}

		// log.Printf("continue %+v commit task : %+v", time.Now().Unix() - startTime.Unix(), cArgs.Task.TaskId)
		success =call("Coordinator.CommitTask", cArgs, cReply)
		if !success || cReply.Code == Exit {
			break
		}
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
	// log.Printf("Worker : %+v Exit", workerId)
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
