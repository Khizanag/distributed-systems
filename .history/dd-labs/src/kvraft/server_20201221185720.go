package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	WaitTime           = 250
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
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB              map[string]string
	resultOf        map[int]chan Op
	lastRequestIDOf map[int64]int64
	killCh          chan bool
}

func (kv *KVServer) initForData(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.resultOf[index]; !ok {
		kv.resultOf[index] = make(chan Op, 1)
	}
}

func (op *Op) isEqual(other *Op) bool {
	return op.ClientID == other.ClientID &&
		op.RequestID == other.RequestID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	entry := Op{
		Key:       args.Key,
		FuncName:  "Get",
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	result := kv.processRequest(entry)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		FuncName:  args.Command,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	result := kv.processRequest(op)
	reply.Err = result.Err
}

func (kv *KVServer) processRequest(entry Op) Op {
	index, _, isLeader := kv.rf.Start(entry)

	resultToReturn := kv.getErrorOp()

	if isLeader {
		kv.initForData(index)

		select {
		case result := <-kv.resultOf[index]:
			if entry.isEqual(&result) {
				resultToReturn = result
			}
		case <-time.After(WaitTime * time.Millisecond):
		}
	}
	return resultToReturn
}

func (kv *KVServer) processAppendGetPutRequest(op Op) Op {
	switch op.FuncName {
	case "Append":
		kv.processAppendRequest(&op)
	case "Get":
		kv.processGetRequest(&op)
	case "Put":
		kv.processPutRequest(&op)
	}
	kv.lastRequestIDOf[op.ClientID] = op.RequestID
	return op
}

func (kv *KVServer) processAppendRequest(op *Op) {
	if kv.isOriginalRequest(op) {
		kv.DB[op.Key] += op.Value
	}
	op.Err = OK
}

func (kv *KVServer) processGetRequest(op *Op) {
	if value, ok := kv.DB[op.Key]; ok {
		op.Err = OK
		op.Value = value
	} else {
		op.Err = ErrNoKey
	}
}

func (kv *KVServer) processPutRequest(op *Op) {
	if kv.isOriginalRequest(op) {
		kv.DB[op.Key] = op.Value
	}
	op.Err = OK
}

func (kv *KVServer) isOriginalRequest(op *Op) bool {
	lastRequestID, ok := kv.lastRequestIDOf[op.ClientID]
	return ok == false || lastRequestID < op.RequestID
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). yErrWrongLeaderou're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- true
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) processApplyMessage(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := applyMsg.Command.(Op)
	result := kv.processAppendGetPutRequest(op)
	kv.clearResultFor(applyMsg.CommandIndex)
	kv.resultOf[applyMsg.CommandIndex] <- result
}

func (kv *KVServer) clearResultFor(index int) {
	if channel, ok := kv.resultOf[index]; ok {
		select {
		case <-channel:
		default: // was empty
		}
	} else {
		kv.resultOf[index] = make(chan Op, 1)
	}
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

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
		select {
		case applyMsg := <-kv.applyCh:
			kv.processApplyMessage(applyMsg)
		case <-kv.killCh:
			break
		}
	}
}

func (kv *KVServer) getErrorOp() Op {
	return Op{
		Err:   ErrWrongLeader,
		Value: "",
	}
}
