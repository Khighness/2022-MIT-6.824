package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// key2shard returns the routed shard according to the key.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// randInt64 generates a rand int value.
func randInt64() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Clerk (client) structure.
type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	makeEnd   func(string) *labrpc.ClientEnd
	clientId  int64
	commandId int64
}

// MakeClerk creates a clerk.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.makeEnd = make_end
	ck.clientId = randInt64()
	ck.commandId = 1
	return ck
}

// Get fetches the current value for the given key.
func (ck *Clerk) Get(key string) string {
	request := KVCommandRequest{
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
	request := KVCommandRequest{
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

// sendRequest sends the request to ShardKV Server and returns the response.
func (ck *Clerk) sendRequest(request KVCommandRequest) KVCommandResponse {
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var response KVCommandResponse
				ok := srv.Call("ShardKV.ExecKVCommand", &request, &response)
				if ok && (response.Err == OK || response.Err == ErrNoKey) {
					ck.commandId += 1
					return response
				}
				if ok && (response.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
