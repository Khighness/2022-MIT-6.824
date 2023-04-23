package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrExecTimeout   = "ErrExecTimeout"
	ErrServerStopped = "ErrServerStopped"
)

type Err string

const (
	MethodGet    = "Get"
	MethodPut    = "Put"
	MethodAppend = "Append"
)

// KVRequest structure.
type KVRequest struct {
	Key       string
	Value     string
	Method    string
	ClientId  int64
	CommandId int64
}

// KVResponse structure.
type KVResponse struct {
	Err   Err
	Value string
}
