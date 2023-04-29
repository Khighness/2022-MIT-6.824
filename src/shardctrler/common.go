package shardctrler

import "6.824/labgob"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

type Err string

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrServer      = "ErrServer"
)

const (
	MethodQuery = "Query"
	MethodJoin  = "Join"
	MethodLeave = "Leave"
	MethodMove  = "Move"
)

var EmptyConfig = Config{}

func init() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// Copy return s new same Config instance.
func (c *Config) Copy() Config {
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}

// IDGroup structure.
type IDGroup struct {
	ClientId  int64
	CommandId int64
}

// JoinArgs structure.
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	IDGroup
}

// JoinReply structure.
type JoinReply struct {
	WrongLeader bool
	Err         Err
}

// LeaveArgs structure.
type LeaveArgs struct {
	GIDs []int
	IDGroup
}

// LeaveReply structure.
type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

// MoveArgs structure.
type MoveArgs struct {
	Shard int
	GID   int
	IDGroup
}

// MoveReply structure.
type MoveReply struct {
	WrongLeader bool
	Err         Err
}

// QueryArgs structure.
type QueryArgs struct {
	Num int // desired config number
	IDGroup
}

// QueryReply structure.
type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
