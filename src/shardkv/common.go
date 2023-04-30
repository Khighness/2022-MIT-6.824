package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	MethodGet    = "Get"
	MethodPut    = "Put"
	MethodAppend = "Append"
)

// Shard structure.
type Shard struct {
	Status int
	Data   map[string]string
}

// KVCommandRequest structure.
type KVCommandRequest struct {
	Key       string
	Value     string
	Method    string
	ClientId  int64
	CommandId int64
}

// KVCommandResponse structure.
type KVCommandResponse struct {
	Err   Err
	Value string
}

// FetchShardRequest structure.
type FetchShardRequest struct {
}

// FetchShardResponse structure.
type FetchShardResponse struct {
}

// CleanShardRequest structure.
type CleanShardRequest struct {
}

// CleanShardResponse structure.
type CleanShardResponse struct {
}
