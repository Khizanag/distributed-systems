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
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	isDebugMode        = false
	channelDefaultSize = 50
)

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

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
	CommandIndex int
	Command      interface{}
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
	role        Role // role of this server: follower, candidate or leader
	currentTerm int
	votedFor    int

	// data for 2B
	log         []LogEntry
	commitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// applyCh             chan ApplyMsg
	// heartbeatReceivedCh chan bool
	// voteGrantedCh       chan bool
	// becameLeaderCh      chan bool
	chanApply     chan ApplyMsg
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool

	voteCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(r.dead)
	e.Encode(r.currentTerm)
	e.Encode(r.votedFor)
	e.Encode(r.log)
	data := w.Bytes()
	r.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (raft *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var dead int32
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&dead) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("Error during decoding information\n")
	} else {
		raft.dead = dead
		raft.currentTerm = currentTerm
		raft.votedFor = votedFor
		raft.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// reject request with stale term number
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// become follower and update current term
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// vote for the candidate
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}
}

//
// check if candidate's log is at least as new as the voter.
//
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if ok {
		if rf.role != Candidate || rf.currentTerm != args.Term {
			// invalid request
			return ok
		}
		if rf.currentTerm < reply.Term {
			// revert to follower state and update current term
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				// win the election
				rf.role = Leader
				rf.persist()
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.chanWinElect <- true
			}
		}
	}

	return ok
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.role == Candidate {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm {
		// reject requests with stale term number
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		// become follower and update current term
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// confirm heartbeat to refresh timeout
	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	baseIndex := rf.log[0].Index

	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict entries before.
		// bypass all entries during the problematic term to speed up.
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if rf.log[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		// otherwise log up to prevLogIndex are safe.
		// merge lcoal log and entries from leader, and apply log if commitIndex changes.
		rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if rf.commitIndex < args.LeaderCommit {
			// update commitIndex and apply log
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

//
// apply log entries with index in range [lastApplied + 1, commitIndex]
//
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.log[i-baseIndex].Command
		rf.chanApply <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.role != Leader || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}
	if reply.Term > rf.currentTerm {
		// become follower and update current term
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.getLastLogIndex())
	}

	baseIndex := rf.log[0].Index
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		// find if there exists an N to update commitIndex
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}

	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

//
// broadcast heartbeat to all followers.
// the heartbeat may be AppendEntries or InstallSnapshot depending on whether
// required log entry is discarded.
//
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index

	for server := range rf.peers {
		if server != rf.me && rf.role == Leader {
			if rf.nextIndex[server] > baseIndex {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				}
				if rf.nextIndex[server] <= rf.getLastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
				}
				args.LeaderCommit = rf.commitIndex

				go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
		newLog := LogEntry{
			Index:   r.getLastLog().Index + 1,
			Term:    r.currentTerm,
			Command: command,
		}
		r.log = append(r.log, newLog)
		r.persist()
		if isDebugMode {
			fmt.Printf("-- Start: Raft#%d: log appended with %v\n", r.me, command)
		}
	}

	return len(r.log) - 1, r.getLastLog().Term, r.role == Leader
}

func (rf *Raft) Run() {
	for {
		switch rf.role {
		case Follower:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.role = Candidate
				rf.persist()
			}
		case Leader:
			go rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * 60)
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				rf.role = Follower
			case <-rf.chanWinElect:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
			}
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (r *Raft) Kill() {
	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
	r.persist()
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
	// persister *Persister, applyCh chan ApplyMsg) *Raft {
	// rf := &Raft{}
	// rf.peers = peers
	// rf.persister = persister
	// rf.me = me

	// // Your initialization code here (2A, 2B, 2C).
	// rf.role = Follower
	// rf.voteCount = 0

	// rf.currentTerm = 0
	// rf.votedFor = -1
	// rf.log = append(rf.log, LogEntry{Term: 0})

	// rf.commitIndex = 0
	// rf.lastApplied = 0

	// rf.chanApply = applyCh
	// rf.chanGrantVote = make(chan bool, 100)
	// rf.chanWinElect = make(chan bool, 100)
	// rf.chanHeartbeat = make(chan bool, 100)

	// // initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// go rf.Run()

	// return rf
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me
	r.dead = 0

	// Your initialization code here 2A
	r.role = Follower
	r.currentTerm = 0
	r.votedFor = -1
	r.voteCount = 0

	// Your initialization code here 2B, 2C).
	r.commitIndex = 0
	r.lastApplied = 0

	zerothLog := LogEntry{
		Index:   0,
		Term:    r.currentTerm,
		Command: nil,
	}
	r.log = []LogEntry{zerothLog}

	// r.applyCh = applyCh
	// r.heartbeatReceivedCh = make(chan bool, channelDefaultSize)
	// r.voteGrantedCh = make(chan bool, channelDefaultSize)
	// r.becameLeaderCh = make(chan bool, channelDefaultSize)
	r.chanApply = applyCh
	r.chanGrantVote = make(chan bool, 100)
	r.chanWinElect = make(chan bool, 100)
	r.chanHeartbeat = make(chan bool, 100)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	go r.Run()

	return r
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (r *Raft) getServersCount() int {
	return len(r.peers)
}

func (r *Raft) getRole(mustBeSafe bool) Role {
	if mustBeSafe {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	return r.role
}

func (r *Raft) getLastLog(mustBeSafe bool) LogEntry {
	if mustBeSafe {
		r.mu.Lock()
		defer r.mu.Unlock()
	}

	return r.log[len(r.log)-1]
}
