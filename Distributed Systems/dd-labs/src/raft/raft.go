package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
// import "fmt"

// import "bytes"
// import "../labgob"


const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	term int
	votedFor int
	state int
	gotHeartbeat bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct{
	Term int
	LeaderId int
}
type AppendEntriesReply struct{
	Success bool
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("AppendEntries\n")
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}
	reply.Success = true
	rf.term = args.Term
	rf.state = FOLLOWER
	rf.gotHeartbeat = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	VoteGranted bool
	Term int
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("chemitermia %v %v-ma txova %v-s momeci xmao  vinc mtxova imis termia %v\n", rf.term , rf.me, args.CandidateId, args.Term)

	if args.Term < rf.term{
		reply.VoteGranted = false
		reply.Term = rf.term
		return
	}
	reply.VoteGranted = true
	if rf.votedFor == -1 || args.Term > rf.term{
		rf.votedFor = args.CandidateId
		rf.term = args.Term
		rf.state = FOLLOWER
	}
	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// fmt.Printf("started election %v\n", rf.me)
	rf.term++
	size := len(rf.peers)
	votes := 1
	rf.votedFor = rf.me
	var wg sync.WaitGroup
	var votesLock sync.Mutex
	term := rf.term
	rf.mu.Unlock()
	for i := 0 ; i < size; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(ind int){
			args := RequestVoteArgs{term, rf.me}
			reply := RequestVoteReply{}
			// fmt.Printf("chemi id %v da vugzavni %v\n", rf.me, ind)
			ok := rf.sendRequestVote(ind, &args, &reply)
			// fmt.Printf("chemi id %v da vugzavni %v da mipasuxa %v\n", rf.me, ind, ok)
			if ok && reply.VoteGranted {
				votesLock.Lock()
				votes++
				votesLock.Unlock()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	rf.mu.Lock()
	// fmt.Printf("started election %v got votes %v\n", rf.me, votes)
	rf.votedFor = rf.me
	if 2 * votes >= size {
		rf.state = LEADER
	}else{
		rf.state = FOLLOWER
	}
	// fmt.Printf("me var %v da archevnebis mere chemi statea %v\n", rf.me, rf.state)
	rf.mu.Unlock()

}

func (rf *Raft) monitor() {
	for{
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == FOLLOWER{
			rf.mu.Lock()
			rf.gotHeartbeat = false
			rf.mu.Unlock()
			electionTimeout := rand.Intn(300) + 300
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

			rf.mu.Lock()
			if !rf.gotHeartbeat {
				rf.state = CANDIDATE
			}
			rf.mu.Unlock()
		}else if state == CANDIDATE{
			rf.startElection()
		}else if state == LEADER {
			rf.sendHeartBeates()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

func (rf *Raft) sendHeartBeates() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.term
	size := len(rf.peers)
	followers := 1
	var wg sync.WaitGroup
	var followersLock sync.Mutex
	for i := 0 ; i < size; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(ind int){
			args := AppendEntriesArgs{term, rf.me}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(ind, &args, &reply)
			if ok{
				if reply.Success{
					followersLock.Lock()
					followers++
					followersLock.Unlock()
				}else {
					// rf.term = reply.Term
					// rf.state = FOLLOWER
					// rf.gotHeartbeat = true
					// rf.votedFor = -1
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	if 2 * followers < size {
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.term = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	// Your initialization code here (2A, 2B, 2C).

	go rf.monitor()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}


// go test -run 2A
// go test -run 2A
// go test -run 2A
// go test -run 2A
// go test -run 2A
// go test -run 2A
// go test -run 2A
// go test -run 2A
// go test -run 2A
go test -run 2A