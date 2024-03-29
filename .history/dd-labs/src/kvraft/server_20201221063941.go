package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	defaultChannelSize = 100
	Debug              = 0
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	FuncName  string
	ClientID  int64
	RequestID int64
	Err       Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	raft    *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB              map[string]string
	resultOf        map[int]chan Op
	lastRequestIDOf map[int64]int64
	killCh          chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key: args.Key,
		//Value: no needed for get
		FuncName:  "get",
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	// kv.appendLogBy(op)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, defaultChannelSize)
	kv.raft = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.DB = make(map[string]string)
	kv.resultOf = make(map[int]chan Op)
	kv.lastRequestIDOf = make(map[int64]int64)
	kv.killCh = make(chan bool, defaultChannelSize)

	go kv.worker()

	return kv
}

func (kv *KVServer) worker() {
	for {
		applyMsg := <-kv.applyCh
		kv.processApplyMessage(applyMsg)
	}
}

func (kv *KVServer) isOriginRequest(op *Op) bool {
	lastRequestID, ok := kv.lastRequestIDOf[op.ClientID]
	return !ok || op.RequestID > lastRequestID
}

func (kv *KVServer) processApplyMessage(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := applyMsg.Command.(Op)
	switch op.FuncName {
	case "append":
		kv.processAppendRequest(&op)
	case "get":
		kv.processGetRequest(&op)
	case "put":
		kv.processPutRequest(&op)
	default:
		fmt.Printf("-- processApplyMessage: Wrong Function Name]\n")
	}

	kv.clearResultOf(applyMsg.CommandIndex)
	kv.resultOf[applyMsg.CommandIndex] <- op
	kv.lastRequestIDOf[op.ClientID] = op.RequestID
}

func (kv *KVServer) processAppendRequest(op *Op) {
	if kv.isOriginRequest(op) {
		kv.DB[op.Key] += op.Value
	}
	op.Err = OK
}

func (kv *KVServer) processGetRequest(op *Op) {
	if value, ok := kv.DB[op.Key]; ok {
		op.Value = value
		op.Err = OK
	} else {
		op.Err = ErrNoKey
	}
}

func (kv *KVServer) processPutRequest(op *Op) {
	if kv.isOriginRequest(op) {
		kv.DB[op.Key] = op.Value
	}
	op.Err = OK
}

func (kv *KVServer) clearResultOf(index int) {
	// TODO below is copyed
	if ch, ok := kv.resultOf[index]; ok {
		select {
		case <-ch: // drain bad data
		default:
		}
	} else {
		kv.resultOf[index] = make(chan Op, 1)
	}
}

func (kv *KVServer) resetResultChannelOf(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.resultOf[index]; !ok {
		kv.resultOf[index] = make(chan Op, 1)
	} else {
		kv.drainResultDataOf(index)
	}
}

func (kv *KVServer) drainResultDataOf(index int) {

}
