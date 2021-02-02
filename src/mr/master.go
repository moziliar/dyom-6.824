package mr

import (
	"github.com/pkg/errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	M *MapTasks
	R *ReduceTasks
}

type TaskState int

const (
	ToStart TaskState = iota
	Doing
	Finished
)

type MapTask struct {
	FilePath string
	State TaskState
	LastAssigned time.Time
}

type ReduceTask struct {
	FilePaths map[string]bool
	State TaskState
	LastAssigned time.Time
}

type MapTasks struct {
	TaskQueue    []*MapTask
	TaskQueuePtr int
	ToDo         int
	Mu           sync.Mutex
}

type ReduceTasks struct {
	TaskQueue   []*ReduceTask
	TaskQueuePtr int
	ToDo         int
	Mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapTask(req *GetMapTaskReq, reply *GetMapTaskResp) error {
	m.M.Mu.Lock()
	defer m.M.Mu.Unlock()

	if m.M.ToDo <= 0 {
		return errors.Errorf("no more map task")
	}

	for m.M.TaskQueue[m.M.TaskQueuePtr].State == Finished ||
		m.M.TaskQueue[m.M.TaskQueuePtr].State == Doing &&
		time.Since(m.M.TaskQueue[m.M.TaskQueuePtr].LastAssigned) < 10 * time.Second {
		m.M.TaskQueuePtr = (m.M.TaskQueuePtr + 1) % len(m.M.TaskQueue)
	}

	reply.T = m.M.TaskQueue[m.M.TaskQueuePtr]
	reply.T.State = Doing
	reply.T.LastAssigned = time.Now()
	reply.TaskIdx = m.M.TaskQueuePtr
	reply.NReduce = len(m.R.TaskQueue)
	m.M.TaskQueuePtr = (m.M.TaskQueuePtr + 1) % len(m.M.TaskQueue)

	return nil
}

func (m *Master) DoneMapTask(args *DoneMapTaskReq, reply *DoneMapTaskResp) error {
	m.M.Mu.Lock()
	defer m.M.Mu.Unlock()
	m.R.Mu.Lock()
	defer m.R.Mu.Unlock()

	m.M.TaskQueue[args.TaskDoneIdx].State = Finished
	m.M.ToDo--

	for _, intermediateFile := range args.IntermediateFiles {
		reduceBucket, err := strconv.Atoi(strings.Split(intermediateFile, "-")[2])
		if err != nil {
			log.Fatalf("parse int error %v", err.Error())
		}

		reduceBucket = reduceBucket % len(m.R.TaskQueue)
		m.R.TaskQueue[reduceBucket].FilePaths[intermediateFile] = true
		m.R.TaskQueue[reduceBucket].State = ToStart
	}

	reply.Success = true

	return nil
}

func (m *Master) GetReduceTask(args *GetReduceTaskReq, reply *GetReduceTaskResp) error {
	m.R.Mu.Lock()
	defer m.R.Mu.Unlock()

	if m.M.ToDo > 0 {
		return nil
	}

	if m.R.ToDo <= 0 {
		return errors.Errorf("no more reduce task")
	}

	for m.R.TaskQueue[m.R.TaskQueuePtr].State == Finished ||
		m.R.TaskQueue[m.R.TaskQueuePtr].State == Doing &&
		time.Since(m.R.TaskQueue[m.R.TaskQueuePtr].LastAssigned) < 10 * time.Second {
		m.R.TaskQueuePtr = (m.R.TaskQueuePtr + 1) % len(m.R.TaskQueue)
	}

	reply.T = m.R.TaskQueue[m.R.TaskQueuePtr]
	reply.T.State = Doing
	reply.T.LastAssigned = time.Now()
	reply.TaskIdx = m.R.TaskQueuePtr
	reply.CanStart = true
	m.R.TaskQueuePtr = (m.R.TaskQueuePtr + 1) % len(m.R.TaskQueue)

	return nil
}

func (m *Master) DoneReduceTask(args *DoneReduceTaskReq, reply *DoneReduceTaskResp) error {
	m.R.Mu.Lock()
	defer m.R.Mu.Unlock()

	m.R.TaskQueue[args.TaskDoneIdx].State = Finished
	m.R.ToDo--

	reply.Success = true

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.M.ToDo <= 0 && m.R.ToDo <= 0 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.M = &MapTasks{
		TaskQueue:    []*MapTask{},
		TaskQueuePtr: 0,
		ToDo        : 0,
		Mu:           sync.Mutex{},
	}

	m.R = &ReduceTasks{
		TaskQueue:    []*ReduceTask{},
		TaskQueuePtr: 0,
		ToDo        : 0,
		Mu:           sync.Mutex{},
	}

	for i := 0; i < nReduce; i++ {
		m.R.TaskQueue = append(
			m.R.TaskQueue,
			&ReduceTask{FilePaths: make(map[string]bool)})
	}

	for _, file := range files {
		m.M.TaskQueue = append(m.M.TaskQueue, &MapTask{
			FilePath: file,
			State: ToStart,
		})
		m.M.ToDo++
	}

	m.R.ToDo = nReduce

	m.server()
	return &m
}
