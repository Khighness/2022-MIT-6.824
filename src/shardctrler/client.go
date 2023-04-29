package shardctrler

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

// Clerk (client) structure.
type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
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
	return ck
}

// Query fetches the config corresponding to the specified num.
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.ClientId = ck.clientId
	args.CommandId = randInt64()
	args.Num = num

	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		ck.sleepOnce()
	}
}

// Join creates a new replication group according to the gid-servers map.
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.ClientId = ck.clientId
	args.CommandId = randInt64()
	args.Servers = servers

	for {
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		ck.sleepOnce()
	}
}

// Leave removes the replication groups according to the gids
// and redistributes the shards of the removed groups to the
// remaining groups.
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.ClientId = ck.clientId
	args.CommandId = randInt64()
	args.GIDs = gids

	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		ck.sleepOnce()
	}
}

// Move moves the specified shard to the replication group corresponding to the gid.
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.ClientId = ck.clientId
	args.CommandId = randInt64()
	args.Shard = shard
	args.GID = gid

	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		ck.sleepOnce()
	}
}

// sleepOnce pauses for a certain amount of time.
func (ck *Clerk) sleepOnce() {
	time.Sleep(100 * time.Millisecond)
}
