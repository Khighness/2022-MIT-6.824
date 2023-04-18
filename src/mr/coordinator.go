package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	scheduleInterval = 500 * time.Millisecond
	taskExecTimeout  = 10 * time.Second
)

// Coordinator structure.
type Coordinator struct {
	files      []string
	nMap       int
	nReduce    int
	taskPhase  TaskPhase
	taskStates []TaskState
	taskQueue  chan Task
	workerSeq  int
	done       bool
	mu         sync.Mutex
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
		panic(fmt.Errorf("listen error: %s", e))
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done
}

// RegisterWorker processes rpc logic of worker registry.
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) {

}

// ApplyTask processes rpc logic that worker applies for task.
func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) {

}

// ApplyTask processes rpc logic that worker report task status.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) {

}

// pollingTask polls all task states.
func (c *Coordinator) pollingTask() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}

	done := true
	for seq, state := range c.taskStates {
		switch state.status {
		case TaskStatusInitial:
			done = false
			c.pushTask(seq)
		case TaskStatusRunning:
			done = false
			if time.Since(state.startTime) > taskExecTimeout {
				c.pushTask(seq)
			}
		}
	}

	if done {
		if c.taskPhase == TaskPhaseMap {
			c.taskPhase = TaskPhaseMap
			c.taskStates = make([]TaskState, c.nReduce)
		} else {
			c.done = true
		}
	}
}

// pushTask push a task into the task queue.
func (c *Coordinator) pushTask(seq int) {
	task := Task{
		Phase:   c.taskPhase,
		NMap:    c.nMap,
		NReduce: c.nReduce,
		Seq:     seq,
	}

	if task.Phase == TaskPhaseMap {
		task.Filename = c.files[seq]
	}

	c.taskStates[seq].status = TaskStatusInitial
	c.taskQueue <- task
}

// popTask pop a task from the task queue.
func (c *Coordinator) popTask(workerId int) Task {
	task := <-c.taskQueue

	c.mu.Lock()
	taskState := c.taskStates[task.Seq]
	taskState.status = TaskStatusRunning
	taskState.workerId = workerId
	taskState.startTime = time.Now()
	c.mu.Unlock()

	return task
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nMap:       len(files),
		nReduce:    nReduce,
		taskPhase:  TaskPhaseMap,
		taskStates: make([]TaskState, len(files)),
		workerSeq:  0,
		done:       false,
	}

	if c.nMap > c.nReduce {
		c.taskQueue = make(chan Task, c.nMap)
	} else {
		c.taskQueue = make(chan Task, c.nReduce)
	}

	c.server()
	return &c
}
