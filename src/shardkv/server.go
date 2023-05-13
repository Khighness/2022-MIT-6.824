package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/log"
	"6.824/raft"
	"6.824/shardctrler"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	heartbeatInternal  = 500 * time.Millisecond
	pullConfigInternal = 100 * time.Millisecond
	scanShardInternal  = 50 * time.Millisecond
	consensusTimeout   = 500 * time.Millisecond
)

// ShardKV structure.
type ShardKV struct {
	mu           sync.RWMutex
	id           int
	gid          int
	rf           *raft.Raft
	dead         int32
	persister    *raft.Persister
	maxRaftState int
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd

	ctrlers    []*labrpc.ClientEnd
	clerk      *shardctrler.Clerk
	state      [shardctrler.NShards]Shard
	appliedMap map[int64]int64
	responseCh map[int]chan interface{}
	config     shardctrler.Config
	lastConfig shardctrler.Config

	logger *zap.SugaredLogger
}

// String String uses for easy logging.
func (kv *ShardKV) String() string {
	return fmt.Sprintf("[gid:%d]-%s", kv.gid, kv.rf)
}

// Kill sets the server to dead and stops the raft.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

// killed checks if the server is killed.
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// isStaleShard checks if the shard is stale.
func (kv *ShardKV) isStaleShard(shard int) bool {
	if kv.config.Shards[shard] != kv.gid {
		return true
	}
	if kv.state[shard].Status == StatusPulling || kv.state[shard].Status == StatusPushing {
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
		kv.logger.Infof("%s Failed to decode server state from persistent snapshot", kv)
		panic("err")
	}
	kv.state = state
	kv.appliedMap = appliedMap
	kv.config = config
	kv.lastConfig = lastConfig
}

// maybeSaveSnapshot saves snapshot if the size of raft state exceeds maxRaftState.
func (kv *ShardKV) maybeSaveSnapshot(index int) {
	if kv.maxRaftState == -1 || kv.persister.RaftStateSize() < kv.maxRaftState {
		return
	}

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(kv.state) != nil || encoder.Encode(kv.appliedMap) != nil ||
		encoder.Encode(kv.config) != nil || encoder.Encode(kv.lastConfig) != nil {
		panic("Failed to encode server state")
	}
	kv.rf.Snapshot(index, buffer.Bytes())
}

// ExecKVCommand executes KV command.
func (kv *ShardKV) ExecKVCommand(request *KVCommandRequest, response *KVCommandResponse) {
	defer kv.logger.Infof("%s Exec kv command, request: %+v, response: %+v", kv, request, response)

	clientId := request.ClientId
	commandId := request.CommandId
	key := request.Key
	value := request.Value
	method := request.Method
	shard := key2shard(request.Key)

	kv.mu.RLock()
	if kv.isStaleShard(shard) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	command := Operation{
		ClientId:  clientId,
		CommandId: commandId,
		Key:       key,
		Value:     value,
		Method:    method,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	appliedCommand, err := kv.consensus(index)
	if err == ErrTimeout {
		response.Err = ErrTimeout
		return
	}

	operation, ok := appliedCommand.(Operation)
	if !ok {
		response.Err = ErrWrongLeader
		return
	}

	appliedClientId := operation.ClientId
	appliedCommandId := operation.CommandId
	if clientId != appliedClientId || commandId != appliedCommandId {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.isStaleShard(shard) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	if request.Method == MethodGet {
		response.Value = kv.state[shard].Get(request.Key)
	}
	kv.mu.RUnlock()
	response.Err = OK
}

// FetchShard returns the data of the shards that need to be migrated.
func (kv *ShardKV) FetchShard(request *FetchShardRequest, response *FetchShardResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.config.Num < request.Num {
		response.Err = ErrConfig
		kv.mu.RUnlock()
		return
	}

	shardList := request.ShardList
	state := make(map[int]Shard)
	for _, shard := range shardList {
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
	kv.mu.RUnlock()
}

// ClearShard clears the data of the shards that have been migrated.
func (kv *ShardKV) ClearShard(request *CleanShardRequest, response *CleanShardResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.config.Num > request.Num {
		response.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	command := ClearShard{
		Num:       request.Num,
		ShardList: request.ShardList,
	}
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		_, response.Err = kv.consensus(index)
	} else {
		response.Err = ErrWrongLeader
	}
}

// handleRaftReady handles the applied messages from Raft.
func (kv *ShardKV) handleRaftReady() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		if applyMsg.SnapshotValid {
			kv.applySnapshot(applyMsg)
		} else if applyMsg.CommandValid {
			kv.applyCommand(applyMsg)
		}

		kv.mu.Unlock()
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
	if command, ok := applyMsg.Command.(ClearShard); ok {
		kv.doApplyClearShard(command)
	}

	if responseCh, ok := kv.responseCh[applyMsg.CommandIndex]; ok {
		responseCh <- applyMsg.Command
	}
	kv.maybeSaveSnapshot(applyMsg.CommandIndex)
}

// doApplyOperation does applying the KV operation.
func (kv *ShardKV) doApplyOperation(command Operation) {
	commandId := command.CommandId
	clientId := command.ClientId
	method := command.Method
	key := command.Key
	value := command.Value
	shard := key2shard(command.Key)

	if !kv.isStaleShard(shard) && kv.appliedMap[clientId] < commandId {
		switch method {
		case MethodPut:
			kv.state[shard].Put(key, value)
		case MethodAppend:
			kv.state[shard].Append(key, value)
		}
		kv.appliedMap[clientId] = commandId
	}
}

// doApplyConfiguration does applying a new configuration.
func (kv *ShardKV) doApplyConfiguration(command Configuration) {
	nextConfig := command.Config
	nextNum := nextConfig.Num
	if nextNum != kv.config.Num+1 {
		return
	}

	kv.logger.Infof("%s Apply config: %+v", kv, command.Config)
	for shard, targetGid := range nextConfig.Shards {
		originGid := kv.config.Shards[shard]
		if targetGid == kv.gid && originGid != kv.gid && originGid != 0 {
			kv.logger.Infof("%s Migrate shard[%d]: %d <- %d", kv, shard, kv.gid, originGid)
			kv.state[shard].Status = StatusPulling
		}
		if targetGid != kv.gid && originGid == kv.gid && targetGid != 0 {
			kv.logger.Infof("%s Migrate shard[%d]: %d -> %d", kv, shard, kv.gid, targetGid)
			kv.state[shard].Status = StatusPushing
		}
	}

	kv.lastConfig = kv.config
	kv.config = nextConfig
}

// doApplyFetchShard does applying the shard.
func (kv *ShardKV) doApplyFetchShard(command FetchShard) {
	state := command.State
	appliedMap := command.AppliedMap
	num := command.Num
	if num != kv.config.Num {
		return
	}

	for shard := range state {
		if kv.state[shard].Status == StatusPulling {
			for k, v := range state[shard].Data {
				kv.state[shard].Put(k, v)
			}
			kv.state[shard].Status = StatusMigrated
		}
	}

	for clientId, commandId := range appliedMap {
		if kv.appliedMap[clientId] < commandId {
			kv.appliedMap[clientId] = commandId
		}
	}
}

// doApplyClearShard does clearing the shard.
func (kv *ShardKV) doApplyClearShard(command ClearShard) {
	shardList := command.ShardList
	num := command.Num
	if num != kv.config.Num {
		return
	}

	for _, shard := range shardList {
		if kv.state[shard].Status == StatusMigrated {
			kv.state[shard].Status = StatusDefault
		}

		if kv.state[shard].Status == StatusPushing {
			kv.state[shard] = Shard{
				Status: StatusDefault,
				Data:   make(map[string]string),
			}
		}
	}
}

// pullConfigPeriodically handles the config if it changes.
func (kv *ShardKV) pullConfigPeriodically() {
	for !kv.killed() {
		kv.mu.RLock()
		shardAllDefault := true
		for _, shard := range kv.state {
			if shard.Status != StatusDefault {
				shardAllDefault = false
				break
			}
		}
		kv.mu.RUnlock()

		_, isLeader := kv.rf.GetState()
		if isLeader && shardAllDefault {
			nextConfig := kv.clerk.Query(kv.config.Num + 1)

			if kv.config.Num+1 == nextConfig.Num {
				command := Configuration{
					Config: nextConfig,
				}

				index, _, isLeader := kv.rf.Start(command)
				if isLeader {
					kv.consensus(index)
				}
			}
		}

		time.Sleep(pullConfigInternal)
	}
}

// broadcastHeartbeat broadcasts heartbeat in raft group.
func (kv *ShardKV) broadcastHeartbeat() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			index, _, isLeader := kv.rf.Start(Operation{})
			if isLeader {
				kv.consensus(index)
			}
		}

		time.Sleep(heartbeatInternal)
	}
}

// scanPullingShards handles the pulling shards.
func (kv *ShardKV) scanPullingShards() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.RLock()
			gidShardList := make(map[int][]int)
			for shard := range kv.state {
				if kv.state[shard].Status == StatusPulling {
					gid := kv.lastConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.RUnlock()

			if len(gidShardList) > 0 {
				kv.logger.Infof("%s Scan pulling shards: %+v", kv, gidShardList)

				var waitGroup sync.WaitGroup
				for gid, shardList := range gidShardList {
					waitGroup.Add(1)
					go kv.sendFetchShard(&waitGroup, gid, shardList)
				}
				waitGroup.Wait()
			}
		}

		time.Sleep(scanShardInternal)
	}
}

// scanMigratedShards handles the migrated shards.
func (kv *ShardKV) scanMigratedShards() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.RLock()
			gidShardList := make(map[int][]int)
			for shard := range kv.state {
				if kv.state[shard].Status == StatusMigrated {
					gid := kv.lastConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.RUnlock()

			if len(gidShardList) > 0 {
				kv.logger.Infof("%s Scan migrated shards: %+v", kv, gidShardList)

				var waitGroup sync.WaitGroup
				for gid, shardList := range gidShardList {
					waitGroup.Add(1)
					go kv.sendClearShard(&waitGroup, gid, shardList)
				}
				waitGroup.Wait()
			}
		}

		time.Sleep(scanShardInternal)
	}
}

// sendFetchShard fetches the data of the shards that need to be migrated from the origin server.
func (kv *ShardKV) sendFetchShard(wg *sync.WaitGroup, gid int, shardList []int) {
	for _, server := range kv.lastConfig.Groups[gid] {
		request := FetchShardRequest{
			Num:       kv.config.Num,
			ShardList: shardList,
		}
		response := FetchShardResponse{}

		ok := kv.makeEnd(server).Call("ShardKV.FetchShard", &request, &response)
		kv.logger.Infof("%s sendFetchShard to %s, request: %+v, response: %+v", kv, server, request, response)

		if ok && response.Err == OK {
			command := FetchShard{
				Num:        response.Num,
				State:      response.State,
				AppliedMap: response.AppliedMap,
			}

			index, _, isLeader := kv.rf.Start(command)
			if isLeader {
				_, response.Err = kv.consensus(index)
			} else {
				response.Err = ErrWrongLeader
			}
			break
		}
	}

	wg.Done()
}

// sendClearShard calls the origin server to clear the shards that have been migrated.
func (kv *ShardKV) sendClearShard(wg *sync.WaitGroup, gid int, shardList []int) {
	for _, server := range kv.lastConfig.Groups[gid] {
		request := CleanShardRequest{
			Num:       kv.config.Num,
			ShardList: shardList,
		}
		response := CleanShardResponse{}

		ok := kv.makeEnd(server).Call("ShardKV.ClearShard", &request, &response)
		kv.logger.Infof("%s sendClearShard to %s, request: %+v, response: %+v", kv, server, request, response)

		if ok && response.Err == OK {
			command := ClearShard{
				Num:       kv.config.Num,
				ShardList: shardList,
			}

			index, _, isLeader := kv.rf.Start(command)
			if isLeader {
				_, response.Err = kv.consensus(index)
			} else {
				response.Err = ErrWrongLeader
			}
			break
		}
	}

	wg.Done()
}

// consensus waits the command corresponding to the index to reach consensus.
func (kv *ShardKV) consensus(index int) (interface{}, Err) {
	responseCh := kv.createResponseCh(index)
	defer kv.removeResponseCh(index)

	select {
	case command := <-responseCh:
		return command, OK
	case <-time.After(consensusTimeout):
		return nil, ErrTimeout
	}
}

// createResponseCh creates a response channel according to command index.
func (kv *ShardKV) createResponseCh(index int) chan interface{} {
	kv.mu.Lock()
	kv.responseCh[index] = make(chan interface{}, 1)
	kv.mu.Unlock()
	return kv.responseCh[index]
}

// removeNotifyReCh removes the response channel according to the command index.
func (kv *ShardKV) removeResponseCh(index int) {
	kv.mu.Lock()
	if broadcastCh, ok := kv.responseCh[index]; ok {
		close(broadcastCh)
		delete(kv.responseCh, index)
	}
	kv.mu.Unlock()
}

// StartServer starts a ShardKV server.
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
// look at appliedMap.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, id int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	kv := new(ShardKV)
	kv.id = id
	kv.gid = gid
	kv.persister = persister
	kv.maxRaftState = maxRaftState
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.makeEnd = makeEnd

	kv.ctrlers = ctrlers
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.appliedMap = make(map[int64]int64)
	kv.responseCh = make(map[int]chan interface{})
	for index := range kv.state {
		kv.state[index] = Shard{
			Status: StatusDefault,
			Data:   make(map[string]string),
		}
	}

	kv.readPersist(kv.persister.ReadSnapshot())
	kv.rf = raft.Make(servers, id, persister, kv.applyCh)

	go kv.handleRaftReady()
	go kv.pullConfigPeriodically()
	go kv.broadcastHeartbeat()
	go kv.scanPullingShards()
	go kv.scanMigratedShards()

	kv.logger = log.NewZapLogger("ShardKV", zapcore.InfoLevel).Sugar()
	kv.logger.Infof("%s ShardKV server [%v] started successfully", kv, id)

	return kv
}
