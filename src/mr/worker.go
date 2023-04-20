package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strings"

	"6.824/log"

	"go.uber.org/zap"
)

// KeyValue represents Key-Value.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey uses to sort KeyValue slice by key.
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

// worker structure.
type worker struct {
	workerId   int
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
	logger     *zap.SugaredLogger
}

// getReduceFileName returns the name of output file in the map phase.
func (w *worker) getReduceFileName(mapId, partitionId int) string {
	return fmt.Sprintf("mr-reduce-%d-%d", mapId, partitionId)
}

// getMergeFileName returns the name of output file in the reduce phase.
func (w *worker) getMergeFileName(partitionId int) string {
	return fmt.Sprintf("mr-merge-%d", partitionId)
}

// start starts worker.
func (w *worker) start() {
	if err := w.register(); err != nil {
		w.logger.Fatal(err)
	}
	for {
		task, err := w.applyTask()
		if err != nil {
			w.logger.Error(err)
		}
		if task != nil {
			w.execTask(*task)
		}
	}
}

// register registers the worker to the coordinator.
func (w *worker) register() error {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Coordinator.RegisterWorker", args, reply); !ok {
		return errors.New("failed to call: RegisterWorker")
	}
	w.workerId = reply.WorkerId
	w.logger.Infof("Register workerId: %d", w.workerId)
	return nil
}

// applyTask applies for task from the coordinator.
func (w *worker) applyTask() (*Task, error) {
	args := &ApplyTaskArgs{WorkerId: w.workerId}
	reply := &ApplyTaskReply{}
	if ok := call("Coordinator.ApplyTask", args, reply); !ok {
		return nil, errors.New("failed to call: ApplyTask")
	}
	w.logger.Infof("Apply for task: %+v", reply.Task)
	return reply.Task, nil
}

// execTask executes a task.
func (w *worker) execTask(task Task) {
	switch task.Phase {
	case TaskPhaseMap:
		w.execMapTask(task)
	case TaskPhaseReduce:
		w.execReduceTask(task)
	}
}

// execMapTask executes a map task.
func (w *worker) execMapTask(task Task) {
	bytes, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		w.logger.Error("Failed to read file: %s, err: %s", task.Filename, err)
		w.reportTask(task, false)
		return
	}

	kvs := w.mapFunc(task.Filename, string(bytes))
	partitions := make([]ByKey, task.NReduce)
	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		partitions[pid] = append(partitions[pid], kv)
	}

	for pid, kvs := range partitions {
		fileName := w.getReduceFileName(task.Seq, pid)
		file, err := os.Create(fileName)
		if err != nil {
			w.logger.Error("Failed to created file: %s, err: %s", fileName, err)
			w.reportTask(task, false)
			return
		}

		encoder := json.NewEncoder(file)
		for _, kv := range kvs {
			if err := encoder.Encode(&kv); err != nil {
				w.logger.Error("Failed to encode kv: %v, err: %s", kv, err)
				w.reportTask(task, false)
				return
			}
		}

		_ = file.Close()
	}

	w.reportTask(task, true)
}

// execReduceTask executes a reduce task.
func (w *worker) execReduceTask(task Task) {
	kvs := make([]KeyValue, 0)

	for i := 0; i < task.NMap; i++ {
		reduceFileName := w.getReduceFileName(i, task.Seq)
		reduceFile, err := os.Open(reduceFileName)
		if err != nil {
			w.logger.Error("Failed to open reduce file: %s, err: %s", reduceFileName, err)
			w.reportTask(task, false)
			return
		}

		decoder := json.NewDecoder(reduceFile)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				w.logger.Error("Failed to decode kv, err: %s", err)
				break
			}
			kvs = append(kvs, kv)
		}
		_ = reduceFile.Close()
	}

	sort.Sort(ByKey(kvs))

	var result []string
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		res := w.reduceFunc(kvs[i].Key, values)
		result = append(result, fmt.Sprintf("%v %v\n", kvs[i].Key, res))
		i = j
	}

	mergeFileName := w.getMergeFileName(task.Seq)
	if err := ioutil.WriteFile(mergeFileName, []byte(strings.Join(result, "")), 0600); err != nil {
		w.logger.Error("Failed to write to merge file: %s, result: %v, err: %s", mergeFileName, result, err)
		w.reportTask(task, false)
		return
	}

	w.reportTask(task, true)
}

// reportTask reports task state to the coordinator.
func (w *worker) reportTask(task Task, done bool) {
	args := &ReportTaskArgs{
		Seq:      task.Seq,
		WorkerId: w.workerId,
		Phase:    task.Phase,
		Done:     done,
	}
	reply := &RegisterReply{}
	if ok := call("Coordinator.ReportTask", args, reply); !ok {
		w.logger.Error("failed to call: ReportTask")
	}
}

// main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {

	worker := worker{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
		logger:     log.NewZapLogger("Worker").Sugar(),
	}

	worker.start()
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
