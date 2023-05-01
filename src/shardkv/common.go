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

type Status int

const (
	StatusDefault = iota
	StatusPull
	StatusPush
	StatusCollection
)

// Shard structure.
type Shard struct {
	Status int
	Data   map[string]string
}

// Get returns the value according to the key.
func (s *Shard) Get(key string) string {
	return s.Data[key]
}

// Put adds a key-value pair.
func (s *Shard) Put(key string, value string) {
	s.Data[key] = value
}

// Append appends a value for a key.
func (s *Shard) Append(key string, value string) {
	s.Data[key] += value
}

// Copy returns the copied data.
func (s *Shard) Copy() map[string]string {
	result := make(map[string]string)
	for k, v := range s.Data {
		result[k] = v
	}
	return result
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
	Num       int
	ShardList []int
}

// FetchShardResponse structure.
type FetchShardResponse struct {
	Err    Err
	Num    int
	State  map[int]Shard
	Client map[int64]int
}

// FetchShard structure.
type FetchShard struct {
	Num    int
	State  map[int]Shard
	Client map[int64]int
}

// CleanShardRequest structure.
type CleanShardRequest struct {
	Num       int
	ShardList []int
}

// CleanShardResponse structure.
type CleanShardResponse struct {
	Err Err
}

// CleanShard structure.
type CleanShard struct {
	ShardList []int
	Num       int
}
