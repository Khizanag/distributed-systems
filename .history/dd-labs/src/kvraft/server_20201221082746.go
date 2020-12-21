package kvraft

import (
	"log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

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

type Result struct {
	Command   string
	OK        bool
	ClientID  int64
	RequestID int64
	Err       Err
	Value     string
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
	resultOf        map[int]chan Result
	lastRequestIDOf map[int64]int64
	killCh          chan bool
}

func (kv *KVServer) appendEntryToLog(entry Op) Result {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.resultOf[index]; !ok {
		kv.resultOf[index] = make(chan Result, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultOf[index]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(240 * time.Millisecond):
		return Result{OK: false}
	}
}

//
// check if the result corresponds to the log entry.
//
func isMatch(entry Op, result Result) bool {
	return entry.ClientID == result.ClientID && entry.RequestID == result.RequestID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.FuncName = "get"
	entry.ClientID = args.ClientID
	entry.RequestID = args.RequestID
	entry.Key = args.Key

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		FuncName:  args.Command,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		Key:       args.Key,
		Value:     args.Value,
	}

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = result.Err
}

func (kv *KVServer) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.ClientID = op.ClientID
	result.RequestID = op.RequestID

	switch op.Command {
	case "put":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] = op.Value
		}
		result.Err = OK
	case "append":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] += op.Value
		}
		result.Err = OK
	case "get":
		if value, ok := kv.data[op.Key]; ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	}
	kv.ack[op.ClientID] = op.RequestID
	return result
}

func (kv *KVServer) isDuplicated(op Op) bool {
	lastRequestID, ok := kv.ack[op.ClientID]
	if ok {
		return lastRequestID >= op.RequestID
	}
	return false
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()

		op := msg.Command.(Op)
		result := kv.applyOp(op)
		if ch, ok := kv.resultCh[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			kv.resultCh[msg.CommandIndex] = make(chan Result, 1)
		}
		kv.resultCh[msg.CommandIndex] <- result
		kv.mu.Unlock()
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Result{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Result)

	go kv.Run()
	return kv
}
