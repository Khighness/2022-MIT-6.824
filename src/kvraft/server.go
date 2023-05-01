package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/log"
	"6.824/raft"

	"go.uber.org/zap"
)

const (
	emptyValue  = ""
	execTimeOut = 500 * time.Millisecond
)

// Op structure.
type Op struct {
	RequestId int64
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
	Method    string
}

// NewOp creates a Op.
func NewOp(request KVRequest) Op {
	return Op{
		RequestId: randInt64(),
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Key:       request.Key,
		Value:     request.Value,
		Method:    request.Method,
	}
}

// Re structure.
type Re struct {
	Err   Err
	Value string
}

// NewRe creates a Re.
func NewRe(err Err, value string) Re {
	return Re{
		Err:   err,
		Value: value,
	}
}

// KVServer structure.
type KVServer struct {
	mu      sync.Mutex
	id      int
	rf      *raft.Raft
	dead    int32 // set by Kill()
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	maxRaftState int             // snapshot if log grows this big
	persister    *raft.Persister // hold this peer's persisted state

	keyValData map[string]string // stores the key-value pair
	appliedMap map[int64]int64   // stores the clientId-commandId pair
	responseCh map[int64]chan Re // stores the requestId-responseCh pair

	logger *zap.SugaredLogger
}

// Kill sets the server to dead and stops the raft.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	kv.logger.Infof("%s KVServer is stopped", kv.rf)
}

// killed checks is the server is killed.
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// readPersist restores state or installs leader's snapshot.
func (kv *KVServer) readPersist(isInit bool, snapshotTerm, snapshotIndex int, snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}

	if !isInit {
		kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, snapshot)
	}

	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)

	var (
		keyValData map[string]string
		appliedMap map[int64]int64
	)

	if decoder.Decode(&keyValData) != nil || decoder.Decode(&appliedMap) != nil {
		kv.logger.Panic("%s Failed to decode server state from persistent snapshot", kv.rf)
	}
	kv.keyValData = keyValData
	kv.appliedMap = appliedMap
}

// maybeSaveSnapshot saves snapshot if the size of raft state exceeds maxRaftState.
func (kv *KVServer) maybeSaveSnapshot(logIndex int) {
	if kv.maxRaftState == -1 || kv.persister.RaftStateSize() < kv.maxRaftState {
		return
	}

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.keyValData) != nil || encoder.Encode(kv.appliedMap) != nil {
		kv.logger.Panic("Failed to encode server state")
	}
	kv.rf.Snapshot(logIndex, buffer.Bytes())
	kv.logger.Infof("%s Snapshot to: %d", kv.rf, logIndex)
}

// ExecCommand executes command from clerk.
func (kv *KVServer) ExecCommand(request *KVRequest, response *KVResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.waitCommand(NewOp(*request), response)
}

// waitCommand waits for command execution to complete.
func (kv *KVServer) waitCommand(op Op, response *KVResponse) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	responseCh := make(chan Re, 1)
	kv.responseCh[op.RequestId] = responseCh
	kv.mu.Unlock()

	timer := time.NewTimer(execTimeOut)
	select {
	case <-kv.stopCh:
		response.Err = ErrServerStopped
	case <-timer.C:
		response.Err = ErrExecTimeout
	case re := <-responseCh:
		response.Err = re.Err
		response.Value = re.Value
	}

	kv.removeResponseCh(op.RequestId)
}

// sendResponse sends response.
func (kv *KVServer) sendResponse(requestId int64, err Err, value string) {
	if ch, ok := kv.responseCh[requestId]; ok {
		ch <- NewRe(err, value)
	}
}

// removeNotifyReCh removes the response channel according to the request id.
func (kv *KVServer) removeResponseCh(reqId int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.responseCh, reqId)
}

// handleRaftReady handles the applied messages from Raft.
func (kv *KVServer) handleRaftReady() {
	for {
		select {
		case <-kv.stopCh:
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.SnapshotValid {
				kv.mu.Lock()
				kv.readPersist(false, applyMsg.SnapshotTerm, applyMsg.CommandIndex, applyMsg.Snapshot)
				kv.mu.Unlock()
			} else {
				kv.handleCommand(applyMsg)
			}
		}
	}
}

// handleCommand handles applied command.
func (kv *KVServer) handleCommand(command raft.ApplyMsg) {
	if !command.CommandValid {
		return
	}

	kv.logger.Infof("%s Apply command: [index=%d, data=%v]", kv.rf, command.CommandIndex, command.Command)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := command.Command.(Op)
	switch op.Method {
	case MethodGet:
		kv.handleGetOp(op)
	case MethodPut, MethodAppend:
		kv.handlePutOrAppendOp(op)
	default:
		kv.logger.Panic("Undefined method: " + op.Method)
	}

	kv.maybeSaveSnapshot(command.CommandIndex)
}

// handleGetOp handles get operation.
func (kv *KVServer) handleGetOp(op Op) {
	if value, ok := kv.keyValData[op.Key]; ok {
		kv.sendResponse(op.RequestId, OK, value)
	} else {
		kv.sendResponse(op.RequestId, ErrNoKey, emptyValue)
	}
}

// handlePutOrAppendOp handles put or append operation.
func (kv *KVServer) handlePutOrAppendOp(op Op) {
	// Ensure idempotence.
	if lastCommandId, ok := kv.appliedMap[op.ClientId]; ok && lastCommandId == op.CommandId {
		kv.sendResponse(op.RequestId, OK, emptyValue)
		return
	}

	switch op.Method {
	case MethodPut:
		kv.keyValData[op.Key] = op.Value
	case MethodAppend:
		if _, ok := kv.keyValData[op.Key]; !ok {
			kv.keyValData[op.Key] = op.Value
		} else {
			kv.keyValData[op.Key] += op.Value
		}
	}

	kv.appliedMap[op.ClientId] = op.CommandId
	kv.sendResponse(op.RequestId, OK, emptyValue)
}

// StartKVServer starts a KVServer.
func StartKVServer(servers []*labrpc.ClientEnd, id int, persister *raft.Persister, maxRaftState int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.id = id
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})

	kv.maxRaftState = maxRaftState
	kv.persister = persister

	kv.keyValData = make(map[string]string)
	kv.appliedMap = make(map[int64]int64)
	kv.responseCh = make(map[int64]chan Re)

	kv.readPersist(true, 0, 0, kv.persister.ReadSnapshot())
	kv.logger = log.NewZapLogger("KVServer", zap.WarnLevel).Sugar()
	kv.rf = raft.Make(servers, id, persister, kv.applyCh)

	go kv.handleRaftReady()

	return kv
}
