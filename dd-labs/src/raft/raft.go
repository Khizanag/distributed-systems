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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// enum that concretes if server is Leader, Candidate or Followers
type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

const (
	MinWaitMSsForRequest = 250
	MaxWaitMSsForRequest = 400
	SleepMSsForLeader    = 110 // max 10 in 1 sec
	SleepMSsForCandidate = 200
	SleepMSsForFollower  = 60

	MaxWaitMSsForElections              = 250
	SleepMSsForCandidateDuringElections = 30
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

type Log struct {
	Index int
	Term  int
	Command interface{}
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
	role                Role // role of this server: follower, candidate or leader
	currentTerm         int
	votedFor            int
	electionRequestTime time.Time // time at which last heartbeat was received

	// data for 2B
	logs []Log
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(r.xxx)
	// e.Encode(r.yyy)
	// data := w.Bytes()
	// r.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (r *Raft) readPersist(data []byte) {
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
	//   r.xxx = xxx
	//   r.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("-- AppendEntries -> Raft #%d before \n", r.me)
	r.mu.Lock()
	defer r.mu.Unlock()

	// fmt.Printf("-- AppendEntries -s> Raft #%d after\n", r.me)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
	} else if len(r.logs) <= args.PrevLogIndex { // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		 // TODO check for logs
		r.role = Follower
		if r.currentTerm < args.Term {
			r.currentTerm = args.Term
			r.votedFor = -1
		}
		r.updateElectionRequestTime()
		reply.Success = true
	}
	// TODO save log
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
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
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("-- RequestVote -> ID: %d candidateID: %d\n", r.me, args.CandidateID)
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
	} else if (args.Term > r.currentTerm)) {
		r.votedFor = -1
	}
		//if r.currentTerm < args.Term || (r.votedFor == -1) || (r.votedFor == args.CandidateID) {
		// ((r.votedFor == -1) || (r.votedFor == args.CandidateID)) && (args.LastLogIndex >= r.getLastLog().Index()) {
		reply.VoteGranted = true
		r.votedFor = args.CandidateID
		r.currentTerm = args.Term
		r.role = Follower
		r.updateElectionRequestTime()
	}
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
func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (r *Raft) watcher() {
	// fmt.Printf("-- Raft #%d watcher started\n", r.me)
	for {
		// fmt.Printf("-- Raft #%d watcher with role %d\n", r.me, r.role)
		role := r.getRoleSafe()
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
	// fmt.Printf("-- Raft #%d watcher finished\n", r.me)
}

func (r *Raft) startElections() {
	// fmt.Printf("-- Raft #%d started Elections before\n", r.me)

	r.mu.Lock()

	r.role = Candidate
	r.currentTerm++
	r.updateElectionRequestTime()

	currentTerm := r.currentTerm

	var numAccepts int32 = 1 // self vote
	r.votedFor = r.me
	var numRejects int32 = 0
	var numServers int32 = int32(r.getServersCount())

	r.mu.Unlock()

	// fmt.Printf("-- Raft #%d started Elections after\n", r.me)

	for i := range r.peers {
		// fmt.Printf("------- i : %d\n", i)
		if i == r.me {
			continue
		}

		go func(index int) { // TODO create private variables and lock only small part of code
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  r.me,
				LastLogIndex: 0, // TODO r.getLastLog().Index,
				LastLogTerm:  0, // TODO r.getLastLog().Term,
			}
			reply := RequestVoteReply{}

			r.sendRequestVote(index, &args, &reply)

			if reply.VoteGranted {
				atomic.AddInt32(&numAccepts, 1)
			} else {
				atomic.AddInt32(&numRejects, 1)

				r.mu.Lock()

				if reply.Term > r.currentTerm { // TODO if is not necessary?
					r.currentTerm = reply.Term
					r.votedFor = -1
					r.updateElectionRequestTime() // TODO ???
				}

				r.mu.Unlock()
			}

		}(i)
	}

	// fmt.Printf("--------------- Raft #%d sent all request votes\n", r.me)

	maxWaitTime := time.Now().Add(time.Duration(MaxWaitMSsForElections) * time.Millisecond)

	for { // wait for other servers

		if atomic.LoadInt32(&numAccepts) > numServers/2 {
			r.mu.Lock()
			r.role = Leader
			// fmt.Printf("------------------------------------------------------------------ Raft #%d became a LEADER\n", r.me)
			r.mu.Unlock()
			r.broadcastHeartBeats()
			break
		} else if atomic.LoadInt32(&numRejects) >= numServers/2 {
			r.mu.Lock()
			r.role = Follower
			r.mu.Unlock()
			break
		} else if time.Now().After(maxWaitTime) { // time is over
			break
		} else {
			time.Sleep(SleepMSsForCandidateDuringElections)
		}
	}

}

// requires raft's mutex locking
func (r *Raft) broadcastHeartBeats() {
	// fmt.Printf("-- Raft #%d is trying to broadcastHeartBeats before\n", r.me)

	r.mu.Lock()
	currentTerm := r.currentTerm
	r.mu.Unlock()

	// fmt.Printf("-- Raft #%d is trying to broadcastHeartBeats after\n", r.me)

	for i := range r.peers {

		if i == r.me { // no race condition, because me is not changing
			continue
		}

		go func(index int) {

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     r.me,
				PrevLogIndex: 0, // TODO r.getLastLog().Index,
				PrevLogTerm:  0, // TODO r.getLastLog().Term,
				// Entries:      []LogEntry{}, // TODO
				LeaderCommit: 0, // TODO
			}

			reply := AppendEntriesReply{}
			r.sendAppendEntries(index, &args, &reply)

			if reply.Success {

			} else {
				r.mu.Lock()
				if reply.Term > r.currentTerm {
					r.currentTerm = reply.Term
					r.votedFor = -1
				}
				r.mu.Unlock()
			}
		}(i)
	}
}

func (r *Raft) tryBecomingCandidate() {
	// fmt.Printf("-- Raft #%d is trying to become Candidate\n", r.me)
	r.mu.Lock()
	electionRequestTime := r.electionRequestTime
	r.mu.Unlock()
	// fmt.Printf("-- Raft #%d is trying to become Candidate -> check IF\n", r.me)
	if time.Now().After(electionRequestTime) { // heartbeat should already be received
		r.startElections()
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
func (r *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role == Leader {
		newLog = Log {
			Index: r.getLastLog().Index + 1,
			Term: r.currentTerm,
			Command: command,
		}
		r.logs = append(r.logs, newLog)
		// TODO r.persist()
	}
	return r.getLastLog().Index, r.getLastLog().Term, r.role == Leader
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
func (r *Raft) Kill() {
	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
}

func (r *Raft) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
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
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me
	r.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	r.role = Follower
	r.currentTerm = 0
	r.votedFor = -1
	r.updateElectionRequestTime()

	go r.watcher()

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())
	// fmt.Printf("-- Raft #%d started\n", me)
	return r
}

//
//
//	-> some small private methods about Raft
//
//

func (r *Raft) getServersCount() int {
	return len(r.peers)
}

func (r *Raft) getRoleSafe() Role {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getRole()
}

func (r *Raft) getRole() Role {
	return r.role
}

func (r *Raft) updateElectionRequestTimeSafe() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateElectionRequestTime()
}

func (r *Raft) updateElectionRequestTime() {
	numMilliSeconds := rand.Intn(MaxWaitMSsForRequest-MinWaitMSsForRequest) + MinWaitMSsForRequest
	r.electionRequestTime = time.Now().Add(time.Duration(numMilliSeconds) * time.Millisecond)
}

func (r *Raft) getLastLogSafe() Log {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getLastLog()
}

func (r *Raft) getLastLog() Log {
	return r.logs[len(r.logs)-1]
}
