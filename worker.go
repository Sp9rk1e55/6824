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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// sort by hashkey
type ByHashKey []KeyValue

func (a ByHashKey) Len() int           { return len(a) }
func (a ByHashKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByHashKey) Less(i, j int) bool { return ihash(a[i].Key) < ihash(a[j].Key) }

//sort by key

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
// key hash code
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// process a map task
func StartMapTask(timestamp int64, reply *ReplyArgs, mapf func(string, string) []KeyValue) bool {
	ifile, err := os.Open(reply.RepContent)
	if err != nil {
		log.Fatalf("can't open %v", reply.RepContent)
	}
	defer ifile.Close()
	content, err := io.ReadAll(ifile)
	if err != nil {
		log.Fatalf("can't read %v", reply.RepContent)
	}
	intermediate := mapf(reply.RepContent, string(content))
	//
	ofile := make([]*os.File, reply.RepnReduce)
	for i := 0; i < reply.RepnReduce; i++ {
		ofname := "mr-" + strconv.Itoa(reply.RepTaskId) + "-" + strconv.Itoa(i)
		ofile[i], _ = os.Create(ofname)
		defer ofile[i].Close()
	}
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % reply.RepnReduce
		enc := json.NewEncoder(ofile[reduceId])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("can't read %v", ofile[reduceId])
		}
	}

	//notice server task finished
	args := ReqArgs{}
	args.ReqID = timestamp
	args.ReqOp = TaskMapDone
	args.ReqTaskId = reply.RepTaskId
	nextreply := ReplyArgs{}
	return call("Coordinator.Request", &args, &nextreply)
}

// process a reduce task
func StartReduceTask(timestamp int64, reply *ReplyArgs, reducef func(string, []string) string) bool {
	kva := []KeyValue{}
	for i := 0; i < reply.RepnMap; i++ {
		ifilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.RepTaskId)
		ifile, err := os.Open(ifilename)
		if err != nil {
			log.Fatalf("Open file failed")
		}
		defer ifile.Close()

		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	ofilename := "mr-out-" + strconv.Itoa(reply.RepTaskId)
	ofile, err := os.Create(ofilename)
	if err != nil {
		log.Fatalf("creat open file failed")
	}
	defer ofile.Close()

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		//correct format for each line of Reduce output
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	//notice server task finished
	args := ReqArgs{}
	args.ReqID = timestamp
	args.ReqOp = TaskReduceDone
	args.ReqTaskId = reply.RepTaskId
	nextreply := ReplyArgs{}
	return call("Coordinator.Request", &args, &nextreply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		timestamp := time.Now().Unix()
		args := ReqArgs{}
		args.ReqID = timestamp
		args.ReqOp = TaskReq
		reply := ReplyArgs{}
		ok := call("Coordinator.Request", &args, &reply)
		if reply.RepID != args.ReqID {
			return
		}
		if ok {
			switch reply.RepOp {
			case TaskMap:
				StartMapTask(timestamp, &reply, mapf)
			case TaskReduce:
				StartReduceTask(timestamp, &reply, reducef)
			case TaskWait:
				time.Sleep(time.Second)
			case TaskDone:
				return
			default:
				return
			}
		} else {
			log.Fatalf("Coordinator Servr has been closed")
			return
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
