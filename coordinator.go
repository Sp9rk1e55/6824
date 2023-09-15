package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks      chan int
	reduceTasks   chan int
	nReduce       int
	nMap          int
	mapRunning    []int64
	reduceRunning []int64
	tasks         []string
	mapCnt        int
	reduceCnt     int
	taskDone      bool
	lock          *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Request(args *ReqArgs, reply *ReplyArgs) error {
	c.lock.L.Lock()
	allDone := c.taskDone
	c.lock.L.Unlock()
	if allDone {
		reply.RepID = args.ReqID
		reply.RepOp = TaskDone
		return nil
	}

	switch args.ReqOp {
	case TaskReq:
		if len(c.mapTasks) > 0 {
			reply.RepID = args.ReqID
			reply.RepOp = TaskMap
			reply.RepTaskId = <-c.mapTasks
			reply.RepnMap = c.nMap
			reply.RepContent = c.tasks[reply.RepTaskId]
			reply.RepnReduce = c.nReduce
			c.lock.L.Lock()
			c.mapRunning[reply.RepTaskId] = args.ReqID
			c.lock.L.Unlock()
			go func(taskid int) {
				time.Sleep(10 * time.Second)
				c.lock.L.Lock()
				if c.mapRunning[taskid] != 1 {
					c.mapTasks <- taskid
				} else {
					c.mapCnt--
				}
				c.lock.L.Unlock()
			}(reply.RepTaskId)
			return nil
		} else if len(c.mapTasks) == 0 {
			c.lock.L.Lock()
			mapCurr := c.mapCnt
			reduceCurr := c.reduceCnt
			c.lock.L.Unlock()

			if mapCurr > 0 {
				reply.RepID = args.ReqID
				reply.RepOp = TaskWait
				return nil
			} else {
				if len(c.reduceTasks) > 0 {
					reply.RepID = args.ReqID
					reply.RepOp = TaskReduce
					reply.RepTaskId = <-c.reduceTasks
					reply.RepnMap = c.nMap
					reply.RepnReduce = c.nReduce
					c.lock.L.Lock()
					c.reduceRunning[reply.RepTaskId] = args.ReqID
					c.lock.L.Unlock()
					go func(taskId int) {
						time.Sleep(10 * time.Second)
						c.lock.L.Lock()
						//defer or ...
						//defer c.lock.L.Unlock()
						if c.reduceRunning[taskId] != 1 {
							c.reduceTasks <- taskId
						} else {
							c.reduceCnt--
							if c.reduceCnt == 0 {
								c.taskDone = true
							}
						}
						c.lock.L.Unlock()
					}(reply.RepTaskId)
				} else {
					if reduceCurr > 0 {
						reply.RepID = args.ReqID
						reply.RepOp = TaskWait
					}
				}
			}
		}
	case TaskMapDone:
		c.lock.L.Lock()
		if c.mapRunning[args.ReqTaskId] == args.ReqID {
			c.mapRunning[args.ReqTaskId] = 1
		}
		c.lock.L.Unlock()
	case TaskReduceDone:
		c.lock.L.Lock()
		if c.reduceRunning[args.ReqTaskId] == args.ReqID {
			c.reduceRunning[args.ReqTaskId] = 1
		}
		c.lock.L.Unlock()
	default:
		return nil
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.L.Lock()
	ret = c.taskDone
	c.lock.L.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make(chan int, len(files))
	c.reduceTasks = make(chan int, nReduce) //nReduce is the number of the reduce tasks to use
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapRunning = make([]int64, len(files))
	c.reduceRunning = make([]int64, nReduce)
	c.tasks = make([]string, len(files))
	c.taskDone = false
	c.mapCnt = len(files)
	c.reduceCnt = nReduce
	c.lock = sync.NewCond(&sync.Mutex{})

	//task will insert to this queue
	for i := 0; i < len(files); i++ {
		c.tasks[i] = files[i]
		c.mapRunning[i] = 0
		c.mapTasks <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- i
		c.reduceRunning[i] = 0
	}
	//start rpc server
	c.server()
	return &c
}
