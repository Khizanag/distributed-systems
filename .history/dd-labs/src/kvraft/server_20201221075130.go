// package kvraft

// import (
// 	"fmt"
// 	"log"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"../labgob"
// 	"../labrpc"
// 	"../raft"
// )

// const (
// 	defaultChannelSize = 100
// 	Debug              = 0
// )

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 0 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

// type Op struct {
// 	// Your definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// 	Key       string
// 	Value     string
// 	FuncName  string
// 	ClientID  int64
// 	RequestID int64
// 	Err       Err
// }

// type KVServer struct {
// 	mu      sync.Mutex
// 	me      int
// 	rf      *raft.Raft
// 	applyCh chan raft.ApplyMsg
// 	dead    int32 // set by Kill()

// 	maxraftstate int // snapshot if log grows this big

// 	// Your definitions here.
// 	DB              map[string]string
// 	resultOf        map[int]chan Op
// 	lastRequestIDOf map[int64]int64
// 	killCh          chan bool
// }

// func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
// 	// Your code here.
// 	op := Op{
// 		Key:       args.Key,
// 		FuncName:  "Get",
// 		ClientID:  args.ClientID,
// 		RequestID: args.RequestID,
// 	}

// 	resultOp := kv.processRequest(op)

// 	reply.Err = resultOp.Err
// 	reply.Value = op.Value
// }

// func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
// 	op := Op{
// 		Key:       args.Key,
// 		Value:     args.Value,
// 		FuncName:  args.Op,
// 		ClientID:  args.ClientID,
// 		RequestID: args.RequestID,
// 	}

// 	resultOp := kv.processRequest(op)
// 	reply.Err = resultOp.Err
// }

// // TODO is copyied
// func (kv *KVServer) isMatch(entry *Op, result *Op) bool {
// 	return entry.ClientID == result.ClientID &&
// 		entry.RequestID == result.RequestID
// }

// func (kv *KVServer) processRequest(op Op) Op {
// 	ok, index := kv.insertRequestInPool(op)

// 	opToRet := kv.getErrorOp()

// 	if ok {
// 		kv.resetResultChannelOf(index)

// 		select {
// 		case resultOp := <-kv.resultOf[index]:
// 			if kv.isMatch(&resultOp, &op) {
// 				opToRet = resultOp
// 			}
// 		case <-time.After(240 * time.Millisecond): // TODO change
// 		}
// 	}

// 	return opToRet
// }

// func (kv *KVServer) getErrorOp() Op {
// 	return Op{
// 		Err:   ErrWrongLeader,
// 		Value: "",
// 	}
// }

// func (kv *KVServer) insertRequestInPool(op Op) (bool, int) {
// 	index, _, isLeader := kv.rf.Start(op)
// 	return isLeader, index
// }

// //
// // the tester calls Kill() when a KVServer instance won't
// // be needed again. for your convenience, we supply
// // code to set rf.dead (without needing a lock),
// // and a killed() method to test rf.dead in
// // long-running loops. you can also add your own
// // code to Kill(). yErrWrongLeaderou're not required to do anything
// // about this, but it may be convenient (for example)
// // to suppress debug output from a Kill()ed instance.
// //
// func (kv *KVServer) Kill() {
// 	atomic.StoreInt32(&kv.dead, 1)
// 	kv.rf.Kill()
// 	// Your code here, if desired.
// }

// func (kv *KVServer) killed() bool {
// 	z := atomic.LoadInt32(&kv.dead)
// 	return z == 1
// }

// //
// // servers[] contains the ports of the set of
// // servers that will cooperate via Raft to
// // form the fault-tolerant key/value service.
// // me is the index of the current server in servers[].
// // the k/v server should store snapshots through the underlying Raft
// // implementation, which should call persister.SaveStateAndSnapshot() to
// // atomically save the Raft state along with the snapshot.
// // the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// // in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// // you don't need to snapshot.
// // StartKVServer() must return quickly, so it should start goroutines
// // for any long-running work.
// //
// func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
// 	// call labgob.Register on structures you want
// 	// Go's RPC library to marshall/unmarshall.
// 	labgob.Register(Op{})

// 	kv := new(KVServer)
// 	kv.me = me
// 	kv.maxraftstate = maxraftstate

// 	// You may need initialization code here.

// 	kv.applyCh = make(chan raft.ApplyMsg, defaultChannelSize)
// 	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

// 	// You may need initialization code here.
// 	kv.DB = make(map[string]string)
// 	kv.resultOf = make(map[int]chan Op)
// 	kv.lastRequestIDOf = make(map[int64]int64)
// 	kv.killCh = make(chan bool, defaultChannelSize)

// 	go kv.worker()

// 	return kv
// }

// func (kv *KVServer) worker() {
// 	for {
// 		applyMsg := <-kv.applyCh
// 		kv.processApplyMessage(applyMsg)
// 	}
// }

// func (kv *KVServer) isOriginRequest(op *Op) bool {
// 	lastRequestID, ok := kv.lastRequestIDOf[op.ClientID]
// 	return !ok || op.RequestID > lastRequestID
// }

// func (kv *KVServer) processApplyMessage(applyMsg raft.ApplyMsg) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	op := applyMsg.Command.(Op)
// 	switch op.FuncName {
// 	case "Append":
// 		kv.processAppendRequest(&op)
// 	case "Get":
// 		kv.processGetRequest(&op)
// 	case "Put":
// 		kv.processPutRequest(&op)
// 	default:
// 		fmt.Printf("-- processApplyMessage: Wrong Function Name {%v} \n", op.FuncName)
// 	}

// 	kv.clearResultOf(applyMsg.CommandIndex)
// 	kv.resultOf[applyMsg.CommandIndex] <- op
// 	kv.lastRequestIDOf[op.ClientID] = op.RequestID
// }

// func (kv *KVServer) processAppendRequest(op *Op) {
// 	if kv.isOriginRequest(op) {
// 		kv.DB[op.Key] += op.Value
// 	}
// 	op.Err = OK
// }

// func (kv *KVServer) processGetRequest(op *Op) {
// 	if value, ok := kv.DB[op.Key]; ok {
// 		op.Value = value
// 		op.Err = OK
// 	} else {
// 		op.Err = ErrNoKey
// 	}
// }

// func (kv *KVServer) processPutRequest(op *Op) {
// 	if kv.isOriginRequest(op) {
// 		kv.DB[op.Key] = op.Value
// 	}
// 	op.Err = OK
// }

// func (kv *KVServer) clearResultOf(index int) {
// 	// TODO below is copyed
// 	if ch, ok := kv.resultOf[index]; ok {
// 		select {
// 		case <-ch: // drain bad data
// 		default:
// 		}
// 	} else {
// 		kv.resultOf[index] = make(chan Op, 1)
// 	}
// }

// func (kv *KVServer) resetResultChannelOf(index int) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	if _, ok := kv.resultOf[index]; !ok {
// 		kv.resultOf[index] = make(chan Op, 1)
// 	} else {
// 		kv.drainResultDataOf(index)
// 	}
// }

// func (kv *KVServer) drainResultDataOf(index int) {

// }

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
	Command   string // "get" | "put" | "append"
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type Result struct {
	Command     string
	OK          bool
	ClientId    int64
	RequestId   int64
	WrongLeader bool
	Err         Err
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     map[string]string   // key-value data
	ack      map[int64]int64     // client's latest request id (for deduplication)
	resultCh map[int]chan Result // log index to result of applying that entry
}

func (kv *KVServer) appendEntryToLog(entry Op) Result {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.resultCh[index]; !ok {
		kv.resultCh[index] = make(chan Result, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultCh[index]:
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
	return entry.ClientId == result.ClientId && entry.RequestId == result.RequestId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "get"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Command:   args.Command,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (kv *KVServer) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId

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
	kv.ack[op.ClientId] = op.RequestId
	return result
}

func (kv *KVServer) isDuplicated(op Op) bool {
	lastRequestId, ok := kv.ack[op.ClientId]
	if ok {
		return lastRequestId >= op.RequestId
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
