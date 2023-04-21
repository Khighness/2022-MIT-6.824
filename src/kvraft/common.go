package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// PutAppendArgs structure.
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int
	CommandId int
}

// PutAppendReply structure.
type PutAppendReply struct {
	Err Err
}

// GetArgs structure.
type GetArgs struct {
	Key       string
	ClientId  int
	CommandId int
}

// GetReply structure.
type GetReply struct {
	Err   Err
	Value string
}
