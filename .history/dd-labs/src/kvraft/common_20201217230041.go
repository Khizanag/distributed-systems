package kvraft

// const (
// 	OK             = "OK"
// 	ErrNoKey       = "ErrNoKey"
// 	ErrWrongLeader = "ErrWrongLeader"
// )

// type Err string

// // Put or Append
// type PutAppendArgs struct {
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

// OK and ErrNoKey constants
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

// Err string type
type Err string

// PutAppendArgs structure for Put or Append Argument
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	ClientID  int64
	RequestID int
}

// PutAppendReply structure for Put or Append Reply
type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

// GetArgs structure for Get Argument
type GetArgs struct {
	Key       string
	ClientID  int64
	RequestID int
}

// GetReply structure for Get Reply
type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
