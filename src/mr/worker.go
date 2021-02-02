package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	// do map task
	for {
		if anymore := DoMapTask(mapf); !anymore {
			break
		}

		time.Sleep(time.Second)
	}

	// do reduce task
	for {
		if anymore := DoReduceTask(reducef); !anymore {
			break
		}

		time.Sleep(time.Second)
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func DoMapTask(mapf func(string, string) []KeyValue) (anymore bool) {
	var requestResp GetMapTaskResp
	success := call("Master.GetMapTask", &GetMapTaskReq{}, &requestResp)
	if !success {
		return
	}

	filename := requestResp.T.FilePath

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	appendedFilePairs := make(map[string]*os.File)
	for _, kv := range kva {
		interFilename := fmt.Sprintf("mr-%d-%d", requestResp.TaskIdx, ihash(kv.Key) % requestResp.NReduce)
		fp, exists := appendedFilePairs[interFilename]
		if !exists {
			tmp, err := ioutil.TempFile(".", fmt.Sprintf("%s-*", interFilename))
			if err != nil {
				log.Fatalf("intermediate file %v write failed with err %s", interFilename, err.Error())
			}

			fp = tmp
			appendedFilePairs[interFilename] = fp
		}

		enc := json.NewEncoder(fp)
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("failed to encode kv %v with err %s", kv, err.Error())
		}
	}

	filesAppended := []string{}
	for fileAppended, tmp := range appendedFilePairs {
		tmp.Close()
		if err := os.Rename(tmp.Name(), fileAppended); err != nil {
			log.Fatalf("failed to rename file tmp file %v with err %s", tmp.Name(), err.Error())
		}

		filesAppended = append(filesAppended, fileAppended)
	}

	doneReq := DoneMapTaskReq{
		TaskDoneIdx:       requestResp.TaskIdx,
		IntermediateFiles: filesAppended,
	}
	doneResp := DoneMapTaskResp{}

	call("Master.DoneMapTask", &doneReq, &doneResp)

	if !doneResp.Success {
		log.Fatalf("task %d done msg failed to send", requestResp.TaskIdx)
	}

	anymore = true
	return
}

func DoReduceTask(reducef func(string, []string) string) (anymore bool) {
	var requestResp GetReduceTaskResp

	success := call("Master.GetReduceTask", &GetReduceTaskReq{}, &requestResp)
	if !success {
		return
	}

	if !requestResp.CanStart {
		return true
	}

	filepaths := requestResp.T.FilePaths
	intermediate := []KeyValue{}
	for filepath := range filepaths {
		intermediateFile, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("intermediate file %v read failed with err %s",
				intermediate, err.Error())
		}
		dec := json.NewDecoder(intermediateFile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("decode error %v", err.Error())
			}
			intermediate = append(intermediate, kv)
		}

		intermediateFile.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", requestResp.TaskIdx)
	ofile, _ := ioutil.TempFile(".", fmt.Sprintf("%s-*", oname))

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	if err := os.Rename(ofile.Name(), oname); err != nil {
		log.Fatalf("rename tmp file %s with error %v", ofile.Name(), err.Error())
	}
	ofile.Close()

	doneReq := DoneReduceTaskReq{
		TaskDoneIdx: requestResp.TaskIdx,
	}

	var doneResp DoneReduceTaskResp

	success = call("Master.DoneReduceTask", &doneReq, &doneResp)
	if !success {
		return
	}

	anymore = true
	return
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
