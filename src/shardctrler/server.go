package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/log"
	"6.824/raft"

	"go.uber.org/zap"
)

const (
	execTimeOut = 500 * time.Millisecond
)

// Op structure.
type Op struct {
	RequestId int64
	CommandId int64
	ClientId  int64
	Args      interface{}
	Method    string
}

// NewOp creates a new Op instance.
func NewOp(commandId, clientId int64, args interface{}, method string) Op {
	return Op{
		RequestId: randInt64(),
		CommandId: commandId,
		ClientId:  clientId,
		Args:      args,
		Method:    method,
	}
}

// Re structure.
type Re struct {
	Err    Err
	Config Config
}

// NewRe creates a new Re instance.
func NewRe(err Err, config Config) Re {
	return Re{
		Err:    err,
		Config: config,
	}
}

// ShardCtrler structure.
type ShardCtrler struct {
	mu      sync.Mutex
	id      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	appliedMap map[int64]int64   // clientId -> last applied commandId
	responseCh map[int64]chan Re // clientId -> response channel

	configs []Config // indexed by config num

	logger *zap.SugaredLogger
}

// Kill sets the server to dead and stops the raft.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// getConfig returns the config corresponding to the num.
// If num is negative or out of bound, it returns the last config.
func (sc *ShardCtrler) getConfig(num int) Config {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	}
	return sc.configs[num].Copy()
}

// Query returns the config corresponding to the specified num.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	if args.Num > 0 && args.Num < len(sc.configs) {
		reply.Config = sc.getConfig(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	re := sc.waitCommand(args.CommandId, args.ClientId, *args, MethodQuery)
	if re.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = re.Err
	reply.Config = re.Config
}

// Join creates a new replication group according to the server map.
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	re := sc.waitCommand(args.CommandId, args.CommandId, *args, MethodJoin)
	if re.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = re.Err
}

// Leave removes the servers according to the gids.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	re := sc.waitCommand(args.CommandId, args.CommandId, *args, MethodLeave)
	if re.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = re.Err
}

// Move moves the server corresponding to the gid to the replication group corresponding to the shard.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	re := sc.waitCommand(args.CommandId, args.CommandId, *args, MethodMove)
	if re.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = re.Err
}

// waitCommand waits for command execution to complete and returns re.
func (sc *ShardCtrler) waitCommand(commandId, clientId int64, args interface{}, method string) (re Re) {
	op := NewOp(commandId, clientId, args, method)
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		re.Err = ErrWrongLeader
		return
	}

	sc.logger.Infof("%s Request method: %s, args: %+v", sc.rf, method, args)
	defer sc.logger.Infof("%s Response: %+v", sc.rf, re)

	sc.mu.Lock()
	responseCh := make(chan Re, 1)
	sc.responseCh[op.RequestId] = responseCh
	sc.mu.Unlock()

	timer := time.NewTimer(execTimeOut)
	select {
	case <-sc.stopCh:
		re.Err = ErrServer
	case <-timer.C:
		re.Err = ErrTimeout
	case re = <-responseCh:
	}

	sc.removeResponseCh(op.RequestId)
	return
}

// sendResponse sends response.
func (sc *ShardCtrler) sendResponse(requestId int64, err Err, config Config) {
	if ch, ok := sc.responseCh[requestId]; ok {
		ch <- NewRe(err, config)
	}
}

// removeNotifyReCh removes the response channel according to the request id.
func (sc *ShardCtrler) removeResponseCh(reqId int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.responseCh, reqId)
}

// handleRaftReady handles the applied messages from Raft.
func (sc *ShardCtrler) handleRaftReady() {
	for {
		select {
		case <-sc.stopCh:
			return
		case applyMsg := <-sc.applyCh:
			if applyMsg.SnapshotValid {
				continue
			} else {
				sc.handleCommand(applyMsg)
			}
		}
	}
}

// handleCommand handles applied command.
func (sc *ShardCtrler) handleCommand(command raft.ApplyMsg) {
	if !command.CommandValid {
		return
	}

	sc.logger.Infof("%s Apply command: [index=%d, data=%v]", sc.rf, command.CommandIndex, command.Command)
	sc.mu.Lock()
	defer sc.mu.Unlock()

	op := command.Command.(Op)
	switch op.Method {
	case MethodQuery:
		sc.handleQueryOp(op)
	default:
		sc.handleUpdateOp(op)
	}
}

// handleQueryOp handles query operation.
func (sc *ShardCtrler) handleQueryOp(op Op) {
	config := sc.getConfig(op.Args.(QueryArgs).Num)
	sc.sendResponse(op.RequestId, OK, config)
}

// handleUpdateOp handles update operation.
func (sc *ShardCtrler) handleUpdateOp(op Op) {
	if lastCommandId, ok := sc.appliedMap[op.ClientId]; ok && lastCommandId == op.CommandId {
		sc.sendResponse(op.RequestId, OK, EmptyConfig)
		return
	}

	sc.logger.Infof("%s Before %s, config: %+v", sc.rf, op.Method, sc.configs)
	defer sc.logger.Infof("%s After %s, config: %+v", sc.rf, op.Method, sc.configs)

	switch op.Method {
	case MethodJoin:
		sc.handleJoinOp(op)
	case MethodLeave:
		sc.handleLeaveOp(op)
	case MethodMove:
		sc.handleMoveOp(op)
	default:
		sc.logger.Panicf("Undefined method: " + op.Method)
	}

	sc.appliedMap[op.ClientId] = op.CommandId
	sc.sendResponse(op.RequestId, OK, EmptyConfig)
}

// handleJoinOp handles join operation.
func (sc *ShardCtrler) handleJoinOp(op Op) {
	config := sc.getConfig(-1)
	config.Num++
	for gid, servers := range op.Args.(JoinArgs).Servers {
		config.Groups[gid] = servers
	}

	sc.adjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

// handleLeaveOp handles leave operation.
func (sc *ShardCtrler) handleLeaveOp(op Op) {
	config := sc.getConfig(-1)
	config.Num++

	for _, gidToDel := range op.Args.(LeaveArgs).GIDs {
		delete(config.Groups, gidToDel)
		for shard, gid := range config.Shards {
			if gid == gidToDel {
				config.Shards[shard] = 0
			}
		}
	}

	sc.adjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

// handleMoveOp handles move operation.
func (sc *ShardCtrler) handleMoveOp(op Op) {
	config := sc.getConfig(-1)
	config.Num++
	args := op.Args.(MoveArgs)
	config.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, config)
}

// adjustConfig adjusts config.
func (sc *ShardCtrler) adjustConfig(config *Config) {
	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	if len(gids) == 0 {
		for shard := range config.Shards {
			config.Shards[shard] = 0
		}
	} else {
		sort.Ints(gids)
		total := len(gids)
		for shard := range config.Shards {
			config.Shards[shard] = gids[shard%total]
		}
	}
}

// StartServer starts a ShardCtrler.
func StartServer(servers []*labrpc.ClientEnd, id int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.id = id
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.stopCh = make(chan struct{})
	sc.rf = raft.Make(servers, id, persister, sc.applyCh)

	sc.appliedMap = make(map[int64]int64)
	sc.responseCh = make(map[int64]chan Re)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.logger = log.NewZapLogger("ShardCtrler", zap.InfoLevel).Sugar()

	go sc.handleRaftReady()

	return sc
}
