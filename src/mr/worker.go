package mr

import (
	"fmt"
	"hash/fnv"
	"net/rpc"
)

// StateMap functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (k ByKey) Len() int           { return len(k) }
func (k ByKey) Less(i, j int) bool { return k[i].Key < k[j].Key }
func (k ByKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by StateMap.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	workerId   int
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
}

// getReduceFileName returns the name of output file in the map phase.
func (w *worker) getReduceFileName(mapId, partitionId int) string {
	return fmt.Sprintf("mr-reduce-%d-%d", mapId, partitionId)
}

// getMergeFileName returns the name of output file in the reduce phase.
func (w *worker) getMergeFileName(partitionId int) string {
	return fmt.Sprintf("mr-merge-%d", partitionId)
}

// register registers the worker to the Coordinator.
func (w *worker) register() {

}

// applyTask applies for task from the Coordinator.
func (w *worker) applyTask() (*Task, error) {

	return nil, nil
}

// reportTask reports task state to the Coordinator.
func (w *worker) reportTask(task Task, done bool) {

}

// main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		panic(fmt.Errorf("dialing: %s", err))
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
