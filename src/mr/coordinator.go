package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Coordinator structure.
type Coordinator struct {
	files      []string
	nReduce    int
	taskPhase  TaskPhase
	taskStates []TaskState
	taskChan   chan Task
	workerSeq  int
	done       bool
	muLock     sync.Mutex
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
	ret := false

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.server()
	return &c
}
