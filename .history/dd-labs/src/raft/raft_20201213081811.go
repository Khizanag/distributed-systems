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
	MinWaitMSsForRequest = 300 // lol
	MaxWaitMSsForRequest = 600
	SleepMSsForLeader    = 110 // max 10 in 1 sec
	SleepMSsForCandidate = 200
	SleepMSsForFollower  = 60

	MaxWaitMSsForElections              = 250
	SleepMSsForCandidateDuringElections = 20
)

const (
	isDebugMode = true
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
	Index   int
	Term    int
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
	logs        []Log
	commitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh chan ApplyMsg
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(r.dead)
	e.Encode(r.currentTerm)
	e.Encode(r.votedFor)
	e.Encode(r.logs)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var dead int32
	var currentTerm int
	var votedFor int
	var logs []Log

	if d.Decode(&dead) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Printf("Error during decoding information\n")
	} else {
		raft.dead = dead
		raft.currentTerm = currentTerm
		raft.votedFor = votedFor
		raft.logs = logs
	}
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderID     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	MismatchIndex int
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if isDebugMode {
		fmt.Printf("-- AppendEntries -> Raft #%d before \n", r.me)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentTerm < args.Term {
		r.currentTerm = args.Term
		r.votedFor = -1
	}

	if args.Term < r.currentTerm || args.LeaderID == r.me { // TODO remove leader check and uncomment block
		if isDebugMode {
			fmt.Printf("-- Raft#%v Didn't Appended Entries from %v -> Reason: args.Term(%v) < r.currentTerm(%v)\n", r.me, args.LeaderID, args.Term, r.currentTerm)
		}
		reply.Term = r.currentTerm
		reply.Success = false
	} else if args.PrevLogIndex < 0 {
		r.role = Follower
		if r.currentTerm < args.Term {
			r.currentTerm = args.Term
			r.votedFor = -1
		}

		if len(args.Entries) > 0 { // TODO using for loop
			r.logs = append(r.logs[:args.PrevLogIndex+1], args.Entries...)
		}

		if args.LeaderCommit > r.commitIndex {
			r.commitIndex = intMin(args.LeaderCommit, r.getLastLog().Index)
		}

		r.updateElectionRequestTime()
		reply.Success = true
		if isDebugMode {
			fmt.Printf("-- Raft#%v Appended Entries: %v from %v, Reason: args.PrevLogIndex < 0\n", r.me, args.Entries, args.LeaderID)
		}
	} else if len(r.logs) <= args.PrevLogIndex {
		if isDebugMode {
			fmt.Printf("-- Raft#%v Didn't Appended Entries from %v -> Reason: len(r.logs) <= args.PrevLogIndex\n", r.me, args.LeaderID)
		}
		reply.Term = r.currentTerm
		reply.Success = false
		reply.MismatchIndex = len(r.logs)
		r.updateElectionRequestTime()
	} else if r.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if isDebugMode {
			fmt.Printf("-- Raft#%v Didn't Appended Entries from %v -> Reason: r.logs[args.PrevLogIndex].Term != args.PrevLogTerm\n", r.me, args.LeaderID)
		}
		reply.Term = r.currentTerm
		reply.Success = false

		for i := args.PrevLogIndex; i > 0; i-- { // TODO i>= ???
			if r.logs[i].Term != r.logs[args.PrevLogIndex].Term {
				reply.MismatchIndex = i + 1
				break
			}
		}
		reply.MismatchIndex = 1
		r.updateElectionRequestTime()
	} else { // receive RPC request
		r.role = Follower
		if r.currentTerm < args.Term {
			r.currentTerm = args.Term
			r.votedFor = -1
		}

		if len(args.Entries) > 0 { // TODO using for loop
			r.logs = append(r.logs[:args.PrevLogIndex+1], args.Entries...)
			// TODO r.persist()
		}

		if args.LeaderCommit > r.commitIndex {
			r.commitIndex = intMin(args.LeaderCommit, r.getLastLog().Index)
		}

		r.updateElectionRequestTime()
		reply.Success = true
		if isDebugMode {
			fmt.Printf("-- Raft#%v Appended Entries: %v from %v\n", r.me, args.Entries, args.LeaderID)
		}
	}

	r.persist()
}

func intMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if isDebugMode {
		fmt.Printf("-- ********** -- ********** RequestVote -> ID: %d candidateID: %d\n", r.me, args.CandidateID)
	}

	if args.LastLogIndex > r.getLastLog().Index && r.votedFor == r.me {
		if isDebugMode {
			fmt.Printf("-- RequestVote: Raft#%v voted for Raft#%v(CustomVote)\n", r.me, args.CandidateID)
		}
		r.acceptVoteRequest(args, reply)
	} else if r.candidateLogIsUpToDate(args.LastLogIndex, args.LastLogTerm) &&
		(r.currentTerm < args.Term ||
			(r.currentTerm == args.Term &&
				(r.votedFor == -1 || r.votedFor == args.CandidateID))) { // TODO check for errors
		// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
		if isDebugMode {
			fmt.Printf("-- RequestVote: Raft #%v voted for Raft #%v\n", r.me, args.CandidateID)
		}
		r.acceptVoteRequest(args, reply)
	} else { //if r.currentTerm < args.Term {
		if isDebugMode {
			fmt.Printf("-- RequestVote: Raft#%v rejected Raft#%v\n", r.me, args.CandidateID)
		}
		r.rejectRequestVote(args, reply)
	}

	r.persist()
}

func (r *Raft) acceptVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = true
	reply.Term = r.currentTerm
	r.votedFor = args.CandidateID
	r.currentTerm = args.Term
	r.role = Follower
	r.updateElectionRequestTime()
}

func (r *Raft) rejectRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = -1
	}
	reply.Term = r.currentTerm
	reply.VoteGranted = false
}

// is not thread safe
func (r *Raft) candidateLogIsUpToDate(lastLogIndex int, lastLogTerm int) bool {
	lastLog := r.getLastLog()
	if lastLog.Term < lastLogTerm {
		return true
	} else if lastLog.Term > lastLogTerm {
		return false
	} else { // lastLog.Term == lastLogTerm
		return lastLog.Index <= lastLogIndex
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
	for !r.killed() {
		role := r.getRoleSafe()
		switch role {
		case Leader:
			r.broadcastHeartBeats()
			time.Sleep(SleepMSsForLeader * time.Millisecond)
		case Candidate:
			r.startElections()
		case Follower:
			time.Sleep(SleepMSsForFollower * time.Millisecond)
			r.tryBecomingCandidate()
		}
	}
}

func (r *Raft) fn() {
	for !r.killed() {
		r.mu.Lock()
		for r.commitIndex > r.lastApplied {
			r.lastApplied++
			if len(r.logs) <= r.lastApplied {
				break
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: r.lastApplied,
				Command:      r.logs[r.lastApplied].Command,
			}
			if isDebugMode {
				fmt.Printf("-- -- -- -- -- fn: Raft#%d committed %v on index: %v\n", r.me, applyMsg.Command, applyMsg.CommandIndex)
			}

			r.applyCh <- applyMsg
		}
		r.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}

}

func (r *Raft) updateAppliedLogs() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		if len(r.logs) <= r.lastApplied {
			break
		}
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      r.logs[r.lastApplied].Command,
			CommandIndex: r.lastApplied,
		}
		r.applyCh <- applyMsg
	}
}

func (r *Raft) startElections() {

	r.mu.Lock()

	r.role = Candidate
	r.currentTerm++
	r.updateElectionRequestTime()
	if isDebugMode {
		fmt.Printf("-- Raft #%d started Elections with: currenTerm: %v		commitIndex: %v		logLen: %v\n", r.me, r.currentTerm, r.commitIndex, len(r.logs))
	}

	currentTerm := r.currentTerm

	var numAccepts int32 = 1 // self vote
	r.votedFor = r.me
	var numRejects int32 = 0
	var numServers int32 = int32(r.getServersCount())
	r.persist()
	r.mu.Unlock()

	for i := range r.peers {
		if i == r.me {
			continue
		}

		if r.role != Candidate {
			break
		}

		go func(index int) {
			if r.role != Candidate {
				return
			}

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  r.me,
				LastLogIndex: r.getLastLogSafe().Index,
				LastLogTerm:  r.getLastLogSafe().Term,
			}
			reply := RequestVoteReply{}

			r.sendRequestVote(index, &args, &reply)

			if reply.VoteGranted {
				atomic.AddInt32(&numAccepts, 1)
			} else {
				atomic.AddInt32(&numRejects, 1)

				r.mu.Lock()

				// r.tryUpdateCurrentTerm(reply.Term)
				if reply.Term > r.currentTerm {

					r.currentTerm = reply.Term
					r.votedFor = -1
					r.persist()
					r.updateElectionRequestTime()
				}

				r.mu.Unlock()
			}
		}(i)
	}

	if isDebugMode {
		fmt.Printf("--------------- Raft #%d sent all request votes\n", r.me)
	}

	if r.role != Candidate {
		return
	}

	maxWaitTime := time.Now().Add(time.Duration(MaxWaitMSsForElections) * time.Millisecond)

	for { // wait for other servers
		if r.role != Candidate {
			break
		}

		if atomic.LoadInt32(&numAccepts) > numServers/2 { // became leader
			r.mu.Lock()
			r.role = Leader
			go r.broadcastHeartBeats()
			if isDebugMode {
				fmt.Printf("-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- > Raft %d became LEADER\n", r.me)
			}
			r.nextIndex = make([]int, len(r.peers))
			r.matchIndex = make([]int, len(r.peers))
			for i := range r.peers {
				r.nextIndex[i] = r.getLastLog().Index + 1
			}
			r.mu.Unlock()
			break
		} else if atomic.LoadInt32(&numRejects) >= numServers/2 { // wasn't chosen
			// r.mu.Lock()
			// r.role = Follower
			// r.mu.Unlock()
			break
		} else if time.Now().After(maxWaitTime) { // time is overs
			break
		} else {
			time.Sleep(SleepMSsForCandidateDuringElections)
		}
	}
}

// requires raft's mutex locking
func (r *Raft) broadcastHeartBeats() {

	var numReceivedBeats int32 = 0

	for i := range r.peers {

		if i == r.me { // use of r.me without lock, no race condition, because me is not changing
			continue
		}

		if r.role != Leader {
			break
		}

		go func(index int) {

			// doing this in loop, because we are waiting for another server to receive out infos
			for {
				r.mu.Lock()
				if r.role != Leader {
					r.mu.Unlock()
					break
				}
				logIndex := r.nextIndex[index] - 1
				if logIndex < 0 {
					logIndex = 0
				}

				args := AppendEntriesArgs{
					Term:         r.currentTerm,
					LeaderID:     r.me,
					PrevLogIndex: logIndex,
					PrevLogTerm:  r.logs[logIndex].Term,
					Entries:      r.logs[r.nextIndex[index]:],
					LeaderCommit: r.commitIndex,
				}
				r.mu.Unlock()

				reply := AppendEntriesReply{}
				status := r.sendAppendEntries(index, &args, &reply)

				r.mu.Lock()
				if status { // && r.currentTerm == args.Term { // TODO remove second check
					if reply.Success {
						r.matchIndex[index] = r.getLastLog().Index
						r.nextIndex[index] = r.matchIndex[index] + 1
						atomic.AddInt32(&numReceivedBeats, 1)
						r.mu.Unlock()
						break // this goroutines job is done
					} else { // TODO about break and lock/unlock
						if reply.Term > r.currentTerm {
							r.currentTerm = reply.Term
							r.role = Follower
							if isDebugMode {
								fmt.Printf("-- Raft #%v became FOLLOWER\n", r.me)
							}
							r.votedFor = -1
							r.persist()
							r.mu.Unlock()
							break // this goroutines job is done
						} else {
							r.nextIndex[index] = reply.MismatchIndex
						}
					}
				} else {
					r.mu.Unlock()
					break
				}
				r.mu.Unlock()
			}
		}(i)
	}

	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&numReceivedBeats) == 0 {
		r.mu.Lock()
		r.role = Candidate
		r.mu.Unlock()
	} else {
		r.updateCommitIndexesSafe()
	}

}

func (r *Raft) updateCommitIndexesSafe() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := r.commitIndex + 1; i < len(r.logs) && r.logs[i].Term <= r.currentTerm; i++ { // TODO remove second check

		if r.countServersThatReceived(i) > r.getServersCount()/2 {
			r.commitIndex = i
		} // TODO I think else break
	}
}

func (r *Raft) countServersThatReceived(i int) int {
	count := 1
	for ip := range r.peers {
		if ip == r.me {
			continue
		}
		if r.matchIndex[ip] >= i {
			count++
		}
	}
	return count
}

func (r *Raft) tryBecomingCandidate() {
	if isDebugMode {
		fmt.Printf("-- Raft #%d is trying to become Candidate\n", r.me)
	}
	r.mu.Lock()
	electionRequestTime := r.electionRequestTime
	r.mu.Unlock()
	if time.Now().After(electionRequestTime) { // heartbeat should already be received
		r.role = Candidate
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
		newLog := Log{
			Index:   len(r.logs),
			Term:    r.currentTerm,
			Command: command,
		}
		r.logs = append(r.logs, newLog)
		r.persist()
		if isDebugMode {
			fmt.Printf("-- Start: Raft#%d: log appended with %v\n", r.me, command)
		}
		return len(r.logs) - 1, r.getLastLog().Term, r.role == Leader
	} else {
		return -1, -1, false
	}
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
	r.updateElectionRequestTime()

	// Your initialization code here 2B, 2C).
	r.commitIndex = 0
	r.lastApplied = 0
	zerothLog := Log{
		Index:   0,
		Term:    r.currentTerm,
		Command: nil,
	}
	// r.logs = append(r.logs, zerothLog)
	r.logs = []Log{zerothLog}

	r.applyCh = applyCh

	go r.watcher()
	go r.fn()

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())
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

// go test -run 2B
