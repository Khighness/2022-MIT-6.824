package shardkv

import "6.824/shardctrler"

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
	ErrServer      = "ErrServer"
	ErrTimeout     = "ErrTimeout"
	ErrConfig      = "ErrConfig"
)

type Err string

const (
	MethodGet    = "Get"
	MethodPut    = "Put"
	MethodAppend = "Append"
)

// Status represents the status of Shard.
type Status int

const (
	// StatusDefault represents the shard can be accessed.
	StatusDefault = iota
	// StatusPulling represents the shard is pulling for data from other peers.
	StatusPulling
	// StatusPushing represents the shard is pushing data to the target peer.
	StatusPushing
	// StatusMigrated represents the shard is migrated to the target peer.
	StatusMigrated
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
	Err        Err
	Num        int
	State      map[int]Shard
	AppliedMap map[int64]int64
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

// UniqueId interface.
type UniqueId interface {
	ID() int64
}

// Command structure.
type Command struct {
	RequestId int64
}

// ID implements UniqueId.
func (c Command) ID() int64 {
	return c.RequestId
}

// NewCommand creates a command instance.
func NewCommand() Command {
	return Command{RequestId: randInt64()}
}

// Operation structure.
type Operation struct {
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
	Method    string
	Command
}

// Configuration structure.
type Configuration struct {
	Config shardctrler.Config
}

// FetchShard structure.
type FetchShard struct {
	Num        int
	State      map[int]Shard
	AppliedMap map[int64]int64
	Command
}

// CleanShard structure.
type CleanShard struct {
	Num       int
	ShardList []int
	Command
}

// Result structure.
type Result struct {
	Err   Err
	Value interface{}
}
