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

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

type Role int // enum that concretes if server is Leader, Candidate or Followers

const (
	Leader Role = iota
	Candidate
	Follower
)

const (
	MinWaitMSsForRequest = 250
	MaxWaitMSsForRequest = 400
	SleepMSsForLeader    = 120
	SleepMSsForCandidate = 50
	SleepMSsForFollower  = 20

	MaxWaitMSsForElections              = 150
	SleepMSsForCandidateDuringElections = 20
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

type LogEntry struct {
	Index int
	Term  int
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	logs                []LogEntry
	role                Role // role of this server: follower, candidate or leader
	currentTerm         int
	votedFor            int
	electionRequestTime time.Time // time at which last heartbeat was received
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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

type AppendEntriesArgs struct {
	Term         int           // leader's term
	LeaderID     int           // so follower can redirect clients
	PrevLogIndex int           // index of log entry immediately preceding new ones
	PrevLogTerm  int           // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int           // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentTerm > args.Term { // TODO check for logs
		reply.Term = r.currentTerm
		reply.Success = false
	} else {
		r.role = Follower
		if r.currentTerm < args.Term {
			r.currentTerm = args.Term
			r.votedFor = -1
		}
		r.electionRequestTime = r.getElectionRequestTime()
		reply.Success = true
	}
	// TODO save log
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
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	voteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("-- RequestVote -> ID: %d candidateID: %d\n", r.me, args.CandidateID)
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.voteGranted = false
	} else if (r.votedFor == -1) || (r.votedFor == args.CandidateID) {
		// ((rf.votedFor == -1) || (rf.votedFor == args.CandidateID)) && (args.LastLogIndex >= rf.getLastLog().Index()) {
		reply.voteGranted = true
		r.votedFor = args.CandidateID
		if args.Term > r.currentTerm {
			r.currentTerm = args.Term
			r.votedFor = -1
		}
		r.role = Follower
		r.electionRequestTime = r.getElectionRequestTime()
	}
}

func (r *Raft) getLastLog() LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.logs[len(r.logs)-1]
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

func (r *Raft) watcher() {
	role := r.getRole()
	for {
		switch role {
		case Leader:
			r.broadcastHeartBeats()
			time.Sleep(SleepMSsForLeader * time.Millisecond)
		case Candidate:
			r.startElections()
			time.Sleep(SleepMSsForCandidate * time.Millisecond)
		case Follower:
			r.tryBecomingCandidate()
			time.Sleep(SleepMSsForFollower * time.Millisecond)
		}
	}
}

func (r *Raft) startElections() {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("-- ")

	var votesLock sync.Mutex
	numAccepts := 1 // self vote
	numRejects := 0
	numServers := r.getServersCount()

	for i := range r.peers {
		if i == r.me {
			continue
		}
		go func(index int) {
			args := RequestVoteArgs{
				Term:         r.currentTerm,
				CandidateID:  r.me,
				LastLogIndex: r.getLastLog().Index,
				LastLogTerm:  r.getLastLog().Term,
			}
			reply := RequestVoteReply{}
			r.sendRequestVote(i, &args, &reply)

			votesLock.Lock()
			defer votesLock.Unlock() // TODO move to the end?

			if reply.voteGranted {
				numAccepts++
			} else {
				numRejects++
				if reply.Term > r.currentTerm { // TODO if is not necessary?
					r.currentTerm = reply.Term
				}
			}
		}(i)
	}

	maxWaitTime := time.Now().Add(time.Duration(MaxWaitMSsForElections) * time.Millisecond)

	for { // wait for other servers
		votesLock.Lock()
		_numAccepts := numAccepts
		_numRejects := numRejects
		votesLock.Unlock()

		if time.Now().After(maxWaitTime) {
			break
		} else if _numAccepts > numServers/2 {
			r.role = Leader
			r.broadcastHeartBeats()
			break
		} else if _numRejects > numServers/2 {
			r.role = Follower
			break
		} else {
			time.Sleep(SleepMSsForCandidateDuringElections)
		}
	}
}

func (r *Raft) broadcastHeartBeats() {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("-- Raft #%d is trying to broadcastHeartBeats\n", r.me)

	for i := range r.peers {
		args := AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     -1, // TODO me?
			PrevLogIndex: r.getLastLog().Index,
			PrevLogTerm:  r.getLastLog().Term,
			// Entries:      []LogEntry{}, // TODO
			LeaderCommit: -1, // TODO
		}
		reply := AppendEntriesReply{}

		r.sendAppendEntries(i, &args, &reply)

		if reply.Success {

		} else {
			if reply.Term > r.currentTerm {
				r.currentTerm = reply.Term
				r.votedFor = -1
			}
		}
	}
}

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) tryBecomingCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if time.Now().After(rf.electionRequestTime) { // heartbeat should already be received
		rf.role = Candidate
		rf.electionRequestTime = rf.getElectionRequestTime()
		rf.startElections()
	}
}

func (rf *Raft) getElectionRequestTime() time.Time {
	numMilliSeconds := rand.Intn(MaxWaitMSsForRequest-MinWaitMSsForRequest) + MinWaitMSsForRequest
	return time.Now().Add(time.Duration(numMilliSeconds) * time.Millisecond)
}

func (rf *Raft) broadcastRequestVotes() {
	args := RequestVoteArgs{}
	for i, _ := range rf.peers {
		reply := RequestVoteReply{}
		rf.sendRequestVote(i, &args, &reply)
	}
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

func (r *Raft) getServersCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.peers)
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
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	go rf.watcher()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Printf("-- Raft #%d started\n", me)
	return rf
}
