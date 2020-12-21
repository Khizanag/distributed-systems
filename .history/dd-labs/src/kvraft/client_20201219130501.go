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
	ID            int64 // random ID of this clerk
	nextRequestID int64 // ID to be used for next request
	mu            sync.Mutex
	prevLeader    int // ID of last responder server
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) getNextRequestID() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.nextRequestID++
	return ck.nextRequestID
}

func (ck *Clerk) updatePrevLeaderTo(prevLeader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.prevLeader = prevLeader
}

func (ck *Clerk) getPreviousLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return ck.prevLeader
}

func (ck *Clerk) getNextLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return (ck.prevLeader + 1) % ck.getServersCount()
}

func (ck *Clerk) getServersCount() int {
	return len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ID = nrand()
	ck.nextRequestID = 0
	ck.prevLeader = 0
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

	leader := ck.getPreviousLeader()

	for {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.updatePrevLeaderTo(leader)
			return reply.Value
		} else {
			leader = (leader + 1) % ck.getServersCount()
		}
	}
}

// TODO check without mutex

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
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.ID,
		RequestID: ck.getNextRequestID(),
	}

	leader := ck.getPreviousLeader()

	for {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.updatePrevLeaderTo(leader)
			return
		} else {
			leader = (leader + 1) % ck.getServersCount()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
