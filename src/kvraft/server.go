package kvraft

import (
	"6.824/log"
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	"go.uber.org/zap"
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
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	stopCh  chan struct{}

	maxRaftState int             // snapshot if log grows this big
	persister    *raft.Persister // hold this peer's persisted state

	keyValData map[string]string // stores the key-value pair
	appliedMap map[int64]int64   // stores the clientId-commandId pair
	responseCh map[int64]chan Re // the channel to send response

	logger *zap.SugaredLogger
}

// lock tries to acquire lock with action log.
func (kv *KVServer) lock() {
	if enableLockLog {
		action := getCalledFunction()
		kv.logger.Infof("%s Try to lock for: %s", kv.rf, action)
		defer kv.logger.Infof("%s Succeed to lock for: %s", kv.rf, action)
	}
	kv.mu.Lock()
}

// lock tries to release lock with action log.
func (kv *KVServer) unlock() {
	kv.mu.Unlock()
	if enableLockLog {
		kv.logger.Infof("%s Succeed to unlock after: %s", kv.rf, getCalledFunction())
	}
}

// Kill sets the server to dead and stop the raft.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	kv.logger.Infof("%s server is stopped", kv.rf)
}

// killed checks is the server is killed.
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.logger.Infof("%s Propose command: [index=%d, data=%v]", kv.rf, index, op)

	kv.lock()
	notifyReCh := make(chan Re, 1)
	kv.responseCh[op.RequestId] = notifyReCh
	kv.unlock()

	re := <-notifyReCh
	kv.removeNotifyReCh(op.RequestId)
	response.Err = re.Err
	response.Value = re.Value
}

// sendResponse sends response.
func (kv *KVServer) sendResponse(requestId int64, err Err, value string) {
	if ch, ok := kv.responseCh[requestId]; ok {
		ch <- NewRe(err, value)
	}
}

// removeNotifyReCh removes the response channel according to the request id.
func (kv *KVServer) removeNotifyReCh(requestId int64) {
	kv.lock()
	defer kv.unlock()
	delete(kv.responseCh, requestId)
}

// handleRaftReady handles the applied messages from Raft.
func (kv *KVServer) handleRaftReady() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.SnapshotValid {
				kv.lock()
				kv.readPersist(false, applyMsg.SnapshotTerm, applyMsg.CommandIndex, applyMsg.Snapshot)
				kv.unlock()
			} else {
				kv.handleCommand(applyMsg)
			}
		case <-kv.stopCh:
			return
		}
	}
}

// handleCommand handles applied command.
func (kv *KVServer) handleCommand(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid {
		return
	}

	kv.lock()
	defer kv.unlock()
	kv.logger.Infof("%s Apply command: [index=%d, data=%v]", kv.rf, applyMsg.CommandIndex, applyMsg.Command)

	op := applyMsg.Command.(Op)
	switch op.Method {
	case MethodGet:
		kv.handleGetOp(op)
	case MethodPut, MethodAppend:
		kv.handlePutOrAppendOp(op, applyMsg.CommandIndex)
	}
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
func (kv *KVServer) handlePutOrAppendOp(op Op, index int) {
	// Ensure idempotence.
	if lastCommandId, ok := kv.appliedMap[op.ClientId]; ok && lastCommandId == op.CommandId {
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
	kv.maybeSaveSnapshot(index)
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

	kv.logger = log.NewZapLogger("KVServer", zap.InfoLevel).Sugar()
	kv.rf = raft.Make(servers, id, persister, kv.applyCh)

	go kv.handleRaftReady()

	return kv
}
