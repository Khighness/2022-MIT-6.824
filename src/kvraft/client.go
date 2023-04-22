package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/log"

	"go.uber.org/zap"
)

const (
	emptyValue    = ""
	retryInterval = 10 * time.Millisecond
)

// Clerk (client) structure.
type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	leaderId int
	logger   *zap.SugaredLogger
}

// randInt64 generates a rand int value.
func randInt64() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk creates a clerk.
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = randInt64()
	ck.logger = log.NewZapLogger("Clerk", zap.DebugLevel).Sugar()
	return ck
}

// Get fetches the current value for the given key.
func (ck *Clerk) Get(key string) string {
	request := KVRequest{
		Key:       key,
		Method:    MethodGet,
		ClientId:  ck.clientId,
		CommandId: randInt64(),
	}

	response := ck.sendRequest(request)
	return response.Value
}

// PutAppend creates a key-value pair if the key does not exist
// and appends the value for the key if the key already exists.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	request := KVRequest{
		Key:       key,
		Value:     value,
		Method:    op,
		ClientId:  ck.clientId,
		CommandId: randInt64(),
	}

	_ = ck.sendRequest(request)
}

// Put creates a key-value pair.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, MethodPut)
}

// Append appends the value for the key.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, MethodAppend)
}

// sendRequest sends the request to KVServer and returns the response.
func (ck *Clerk) sendRequest(request KVRequest) (response KVResponse) {
	leaderId := ck.leaderId
	for {
		if ok := ck.servers[leaderId].Call("KVServer.ExecCommand", &request, &response); !ok {
			ck.logger.Warnf("Failed to send request: %+v", request)
		}

		switch response.Err {
		case OK:
			ck.leaderId = leaderId
			return
		case ErrNoKey:
			ck.leaderId = leaderId
			return
		default:
			time.Sleep(retryInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}
