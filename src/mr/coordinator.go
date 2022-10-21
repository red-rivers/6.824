package mr

import (
	// "fmt"
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var(
	MAP TaskType = 1
	REDUCE TaskType = 2
) 

type IdGenerator struct {
	Id int64
	mutex *sync.Mutex
}

func (g *IdGenerator) GenId() int64 {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	atomic.AddInt64(&g.Id, 1)
	return g.Id
}

func NewIdGenerator() *IdGenerator {
	g := &IdGenerator{}
	g.mutex = &sync.Mutex{}
	return g
}

type TimeOutInfo struct {
	taskId 			int64
	workerId        int64
}

type Coordinator struct {
	lock 			*sync.Mutex
	
	status          int
	nReduce         int
	nMap            int
	avaliableTask   chan Task
	distributeTask  map[int64]Task
	timeOutChannel 	chan TimeOutInfo
	timeOutWorker	map[int64]int64
	workerIdGenerator *IdGenerator
	done            chan int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	
	// log.Printf("FetchTask call args: %+v, reply: %+v", args, reply)
	if args.WorkId == 0 {
		reply.WorkId = c.workerIdGenerator.GenId()
	}else {
		reply.WorkId = args.WorkId
	}
	c.lock.Lock()
	// fmt.Printf("the rest of task : %+v\n", len(c.avaliableTask))
	if len(c.avaliableTask) > 0{
		task := <-c.avaliableTask

		reply.Task = &task
		c.distributeTask[reply.Task.TaskId] = task
	}else {
		reply.Code = WaitReduce
		if len(c.distributeTask) == 0 && c.status == int(REDUCE){
			// log.Printf("worker : %+v reply exit", reply.WorkId)
			reply.Code = Exit // means all map task and reduce task has distributed, the worker can exit
		}
	}
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	c.lock.Unlock()
	// log.Printf("reply : %+v", reply)
	// watch the task
	if reply.Code == 0{
		go func(taskId, wokerId int64){
		// log.Printf("worker: %+v, startTime : %+v",reply.WorkId, time.Now())
			time.Sleep(time.Second * 10)
			// log.Printf("worker: %+v, endTime : %+v",reply.WorkId, time.Now())
			c.lock.Lock()
			info := TimeOutInfo{
				taskId: taskId,
				workerId: wokerId,
			}
			c.timeOutChannel <- info
			c.lock.Unlock()
		}(reply.Task.TaskId, reply.WorkId)
	}
	return nil
}

func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {
	
	c.lock.Lock()
	defer c.lock.Unlock()
	// log.Printf("CommitTask call : %+v", args)
	if taskId, ok := c.timeOutWorker[args.WorkId]; ok&&taskId==args.Task.TaskId{
		return nil
	}
	delete(c.distributeTask, args.Task.TaskId)
	if args.Task.TaskType == MAP{
		// log.Printf("map task : %+v, info: %+v has commit", args.Task, args)
		for i:=0; i<c.nReduce; i++{
			err := os.Rename(tmpMapOutputFile(args.WorkId, args.Task.TaskId, i), finalMapOutputFile(args.Task.TaskId, i))
			if err != nil {
				log.Println(err)
				reply.Code = WaitReduce
				return nil
			}
		}
	}
	if args.Task.TaskType == REDUCE{
		// log.Printf("reduce task : %+v, info: %+v has commit", args.Task, args)
		err := os.Rename(tmpReduceOutputFile(args.WorkId, int(args.Task.TaskId)), finalReduceOutputFile(int(args.Task.TaskId)))
		if err != nil {
			log.Panicln(err)
			reply.Code = WaitReduce
			return nil
		}
	}
	// fmt.Printf("%d %d %d\n",len(c.distributeTask), len(c.avaliableTask), c.status)
	if len(c.distributeTask) == 0 && len(c.avaliableTask) == 0{
		if c.status == int(MAP){
			// log.Println("change status")
			c.status = int(REDUCE)
			for i:=0; i<c.nReduce; i++{
				task := Task{}
				task.TaskType = REDUCE
				task.TaskId = int64(i)
				c.avaliableTask <- task
			}
		}else{
			// fmt.Println("DONE")
			c.done <- 1
		}
	}
	return nil
}

// watch the timeout worker
func (c *Coordinator) WatchTask(){
	for info := range c.timeOutChannel{
		c.lock.Lock()
		if task, ok := c.distributeTask[info.taskId]; ok{
			// log.Printf("worker : %+v timeOut, task : %+v", info.workerId, task)
			delete(c.distributeTask, info.taskId)
			c.timeOutWorker[info.workerId] = info.taskId
			// c.timeOutTask[task.TaskId] = workerId
			c.avaliableTask <- task
		}
		c.lock.Unlock()
	}
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
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for ;; {
		select {
		case <-c.done:{
			// fmt.Println("Done Done")
			time.Sleep(time.Second * 3)
			return true
		}
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//init
	
	maxLen := max(len(files), nReduce)
	c := Coordinator{}
	c.avaliableTask = make(chan Task, maxLen)
	// Your code here.
	c.distributeTask = make(map[int64]Task)
	c.timeOutChannel = make(chan TimeOutInfo, maxLen)
	c.timeOutWorker = make(map[int64]int64)
	c.workerIdGenerator = NewIdGenerator()
	c.status = int(MAP)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.lock = &sync.Mutex{}
	c.done = make(chan int)

	for i, file := range files{
		task := Task{}
		task.FileName = file
		task.TaskId = int64(i)
		task.TaskType = MAP
		c.avaliableTask <- task
	}

	go c.WatchTask()
	c.server()
	return &c
}

func max(x, y int) int{
	if x > y {
		return x
	}
	return y
}