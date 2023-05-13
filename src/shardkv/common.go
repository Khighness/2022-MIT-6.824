package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
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
	// StatusPulling represents the server that currently owns the shard is pulling for data from origin server.
	StatusPulling
	// StatusPushing represents the origin server that lastly owns shard is pushing data to the target server.
	StatusPushing
	// StatusMigrated represents the shard is migrated to the target server.
	StatusMigrated
)

func init() {
	labgob.Register(Operation{})
	labgob.Register(Configuration{})
	labgob.Register(FetchShard{})
	labgob.Register(ClearShard{})
}

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

// ExecKVCommand structure.
type Operation struct {
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
	Method    string
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
}

// ClearShard structure.
type ClearShard struct {
	Num       int
	ShardList []int
}

// Result structure.
type Result struct {
	Err   Err
	Value interface{}
}
