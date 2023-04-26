package shardctrler

import (
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

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
}

// Op structure.
type Op struct {
	RequestId int64
	CommandId int64
	ClientId  int64
	Args      interface{}
	Method    string
}

// Re structure.
type Re struct {
	Err    Err
	Config Config
}

// Query returns the config corresponding to the specified num.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

}

// Join creates a new replication group according to the server map.
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

}

// Leave removes the servers according to the gids.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

}

// Move moves the server corresponding to the gid to the replication group corresponding to the shard.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

}

// Kill sets the server and raft to dead and.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer creates a new server.
func StartServer(servers []*labrpc.ClientEnd, id int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.id = id

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.stopCh = make(chan struct{})
	sc.rf = raft.Make(servers, id, persister, sc.applyCh)

	sc.appliedMap = make(map[int64]int64)
	sc.responseCh = make(map[int64]chan Re)

	return sc
}
