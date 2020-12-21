package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID            int64
	nextRequestID int64
	prevLeader    int
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

	for {
		reply := GetReply{}
		ok := ck.servers[ck.prevLeader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			return reply.Value
		}
		ck.updateLeader()
	}
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

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.prevLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			return
		}

		ck.updateLeader()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// ################################# Helper Functions ###########################
func (ck *Clerk) getNextRequestID() int64 {
	ck.nextRequestID++
	return ck.nextRequestID
}

func (ck *Clerk) getNextLeader() int {
	return (ck.prevLeader + 1) % ck.getServersCount()
}

func (ck *Clerk) getServersCount() int {
	return len(ck.servers)
}
func (ck *Clerk) updateLeader() {
	ck.prevLeader = ck.getNextLeader()
}
