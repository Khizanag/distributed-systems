package kvraft

import (
	"log"
	"sync"
	"time"

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

func (kv *KVServer) appendEntryToLog(entry Op) Op {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Op{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	if _, ok := kv.resultOf[index]; !ok {
		kv.resultOf[index] = make(chan Op, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultOf[index]:
		if isMatch(entry, result) {
			return result
		}
		return Op{Err: ErrWrongLeader}
	case <-time.After(240 * time.Millisecond):
		return Op{Err: ErrWrongLeader}
	}
}

//
// check if the result corresponds to the log entry.
//
func isMatch(entry Op, result Op) bool {
	return entry.ClientID == result.ClientID && entry.RequestID == result.RequestID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Key:       args.Key,
		FuncName:  "Get",
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	result := kv.appendEntryToLog(entry)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Key:       args.Key,
		Value:     args.Value,
		FuncName:  args.Command,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	result := kv.appendEntryToLog(entry)
	reply.Err = result.Err
}

func (kv *KVServer) applyOp(op Op) Op {
	result := Op{
		FuncName:  op.FuncName,
		Key:       op.Key,
		ClientID:  op.ClientID,
		RequestID: op.RequestID,
		Err:       OK,
	}

	switch op.FuncName {
	case "Put":
		if !kv.isDuplicated(op) {
			kv.DB[op.Key] = op.Value
		}
		result.Err = OK
	case "Append":
		if !kv.isDuplicated(op) {
			kv.DB[op.Key] += op.Value
		}
		result.Err = OK
	case "Get":
		if value, ok := kv.DB[op.Key]; ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	}
	kv.lastRequestIDOf[op.ClientID] = op.RequestID
	return result
}

func (kv *KVServer) isDuplicated(op Op) bool {
	lastRequestID, ok := kv.lastRequestIDOf[op.ClientID]
	if ok {
		return lastRequestID >= op.RequestID
	}
	return false
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) worker() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()

		op := msg.Command.(Op)
		result := kv.applyOp(op)
		if ch, ok := kv.resultOf[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			kv.resultOf[msg.CommandIndex] = make(chan Op, 1)
		}
		kv.resultOf[msg.CommandIndex] <- result
		kv.mu.Unlock()
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
