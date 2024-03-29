package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu            sync.Mutex
	ID            int64
	nextRequestID int64
	leader        int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ID = nrand()
	ck.nextRequestID = 0
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientID:  ck.ID,
		RequestID: ck.getNextRequestID(),
	}

	// for ; ; ck.leader = (ck.leader + 1) % len(ck.servers) {
	// 	server := ck.servers[ck.leader]
	// 	reply := GetReply{}
	// 	ok := server.Call("KVServer.Get", &args, &reply)
	// 	if ok && reply.Err != ErrWrongLeader {
	// 		return reply.Value
	// 	}
	// }
	for leader := ck.getPreviousLeader(); ; leader = (leader + 1) % ck.getServersCount() {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.updatePrevLeaderTo(leader)
			return reply.Value
		}
	}
}

func (ck *Clerk) getNextRequestID() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.nextRequestID++
	return ck.nextRequestID
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Command:   op,
		ClientID:  ck.ID,
		RequestID: ck.getNextRequestID(),
	}

	for ; ; ck.leader = (ck.leader + 1) % len(ck.servers) {
		server := ck.servers[ck.leader]
		reply := PutAppendReply{}
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "append")
}
