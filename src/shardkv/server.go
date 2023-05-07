package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"

	"go.uber.org/zap"
)

const (
	emptyValue  = ""
	execTimeOut = 500 * time.Millisecond
)

// ShardKV structure.
type ShardKV struct {
	mu      sync.Mutex
	id      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	maxRaftState int // snapshot if log grows this big
	persister    *raft.Persister

	makeEnd func(string) *labrpc.ClientEnd
	gid     int
	ctrlers []*labrpc.ClientEnd
	clerk   *shardctrler.Clerk

	state      [shardctrler.NShards]Shard
	appliedMap map[int64]int64
	responseCh map[int64]chan Result
	config     shardctrler.Config
	lastConfig shardctrler.Config

	logger *zap.SugaredLogger
}

// Kill sets the server to dead and stops the raft.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	kv.logger.Infof("%s ShardKV Server is stopped", kv.rf)
}

// killed checks is the server is killed.
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// isStableShard checks if the shard is stale.
func (kv *ShardKV) isStableShard(shard int) bool {
	if kv.config.Shards[shard] != kv.gid {
		return true
	}
	if kv.state[shard].Status == StatusPull || kv.state[shard].Status == StatusPush {
		return true
	}
	return false
}

// readPersist recovers state by snapshot.
func (kv *ShardKV) readPersist(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}

	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)

	var (
		state      [shardctrler.NShards]Shard
		appliedMap map[int64]int64
		config     shardctrler.Config
		lastConfig shardctrler.Config
	)

	if decoder.Decode(&state) != nil || decoder.Decode(&appliedMap) != nil ||
		decoder.Decode(&config) != nil || decoder.Decode(&lastConfig) != nil {
		kv.logger.Panic("%s Failed to decode server state from persistent snapshot", kv.rf)
	}
	kv.state = state
	kv.appliedMap = appliedMap
	kv.config = config
	kv.lastConfig = lastConfig
}

// maybeSaveSnapshot saves snapshot if the size of raft state exceeds maxRaftState.
func (kv *ShardKV) maybeSaveSnapshot(logIndex int) {
	if kv.maxRaftState == -1 || kv.persister.RaftStateSize() < kv.maxRaftState {
		return
	}

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.state) != nil || encoder.Encode(kv.appliedMap) != nil ||
		encoder.Encode(kv.config) != nil || encoder.Encode(kv.lastConfig) != nil {
		kv.logger.Panic("Failed to encode server state")
	}
	kv.rf.Snapshot(logIndex, buffer.Bytes())
	kv.logger.Infof("%s Snapshot to: %d", kv.rf, logIndex)
}

// FetchShard fetches shard.
func (kv *ShardKV) FetchShard(request *FetchShardRequest, response *FetchShardResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < request.Num {
		response.Err = ErrConfig
		return
	}

	state := make(map[int]Shard)
	for _, shard := range request.ShardList {
		state[shard] = Shard{
			Data: kv.state[shard].Copy(),
		}
	}

	appliedMap := make(map[int64]int64)
	for clientId, commandId := range kv.appliedMap {
		appliedMap[clientId] = commandId
	}

	response.Err = OK
	response.Num = kv.config.Num
	response.State = state
	response.AppliedMap = appliedMap
}

// ClearShard clears shard.
func (kv *ShardKV) ClearShard(request CleanShardRequest, response *CleanShardResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < request.Num {
		response.Err = OK
		return
	}

	command := CleanShard{
		Num:       request.Num,
		ShardList: request.ShardList,
		Command:   NewCommand(),
	}
	re := kv.proposeCommand(command)
	response.Err = re.Err
}

// ExecKVCommand executes KV command.
func (kv *ShardKV) ExecKVCommand(request *KVCommandRequest, response *KVCommandResponse) {
	shard := key2shard(request.Key)

	kv.mu.Lock()
	if kv.isStableShard(shard) {
		response.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	command := Operation{
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Key:       request.Key,
		Value:     request.Value,
		Method:    request.Method,
		Command:   NewCommand(),
	}
	re := kv.proposeCommand(command)
	response.Err = re.Err
	response.Value = re.Value.(string)
}

// proposeCommand proposes a command to leader and waits for the command execution to complete.
func (kv *ShardKV) proposeCommand(cmd UniqueId) (re Result) {
	if _, _, isLeader := kv.rf.Start(cmd); !isLeader {
		re.Err = ErrWrongLeader
		return
	}

	responseCh := kv.createResponseCh(cmd.ID())
	defer kv.removeResponseCh(cmd.ID())

	select {
	case <-kv.stopCh:
		re.Err = ErrServer
	case <-time.After(execTimeOut):
		re.Err = ErrTimeout
	case re = <-responseCh:
	}
	return
}

// createResponseCh creates a response channel according to the request Id.
func (kv *ShardKV) createResponseCh(requestId int64) chan Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	responseCh := make(chan Result, 1)
	kv.responseCh[requestId] = responseCh
	return responseCh
}

// removeNotifyReCh removes the response channel according to the request id.
func (kv *ShardKV) removeResponseCh(requestId int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.responseCh, requestId)
}

// sendResponse sends response.
func (kv *ShardKV) sendResponse(requestId int64, err Err, value interface{}) {
	if ch, ok := kv.responseCh[requestId]; ok {
		ch <- Result{
			Err:   err,
			Value: value,
		}
	}
}

// handleRaftReady handles the applied messages from Raft.
func (kv *ShardKV) handleRaftReady() {
	for {
		select {
		case <-kv.stopCh:
			return
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()

			if applyMsg.SnapshotValid {
				kv.applySnapshot(applyMsg)
			} else if applyMsg.CommandValid {
				kv.applyCommand(applyMsg)
			}

			kv.mu.Unlock()
		}
	}
}

// applySnapshot applies the snapshot.
func (kv *ShardKV) applySnapshot(applyMsg raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
		kv.readPersist(applyMsg.Snapshot)
	}
}

// applyCommand applies the command.
func (kv *ShardKV) applyCommand(applyMsg raft.ApplyMsg) {
	if command, ok := applyMsg.Command.(Operation); ok {
		kv.doApplyOperation(command)
	}
	if command, ok := applyMsg.Command.(Configuration); ok {
		kv.doApplyConfiguration(command)
	}
	if command, ok := applyMsg.Command.(FetchShard); ok {
		kv.doApplyFetchShard(command)
	}
	if command, ok := applyMsg.Command.(CleanShard); ok {
		kv.doApplyClearShard(command)
	}

	cmd := applyMsg.Command.(UniqueId)
	kv.sendResponse(cmd.ID(), OK, applyMsg.Command)
	kv.maybeSaveSnapshot(applyMsg.SnapshotIndex)
}

// doApplyOperation does applying the KV operation.
func (kv *ShardKV) doApplyOperation(command Operation) {
	// Prevent stale read.
	shard := key2shard(command.Key)
	if kv.isStableShard(shard) {
		return
	}

	// Ensure idempotence.
	if lastCommandId, ok := kv.appliedMap[command.ClientId]; ok && lastCommandId == command.CommandId {
		return
	}

	switch command.Method {
	case MethodPut:
		kv.state[shard].Put(command.Key, command.Value)
	case MethodAppend:
		kv.state[shard].Append(command.Key, command.Value)
	}
	kv.appliedMap[command.ClientId] = command.CommandId
}

// doApplyConfiguration does applying the last configuration.
func (kv *ShardKV) doApplyConfiguration(command Configuration) {

}

// doApplyFetchShard does applying the shard.
func (kv *ShardKV) doApplyFetchShard(command FetchShard) {

}

// doApplyClearShard does clearing the shard.
func (kv *ShardKV) doApplyClearShard(command CleanShard) {

}

//
// servers[] contains the ports of the servers in this group.
//
// id is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, id int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Operation{})
	labgob.Register(FetchShard{})
	labgob.Register(CleanShard{})
	labgob.Register(Result{})

	kv := new(ShardKV)
	kv.id = id
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})

	kv.maxRaftState = maxRaftState
	kv.persister = persister

	kv.makeEnd = makeEnd
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.appliedMap = make(map[int64]int64)
	kv.responseCh = make(map[int64]chan Result)
	for idx := range kv.state {
		kv.state[idx] = Shard{
			Status: StatusDefault,
			Data:   make(map[string]string),
		}
	}

	kv.readPersist(kv.persister.ReadSnapshot())
	kv.rf = raft.Make(servers, id, persister, kv.applyCh)

	return kv
}
