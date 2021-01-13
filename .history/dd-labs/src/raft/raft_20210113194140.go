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

	"../labgob"
	"../labrpc"
)

const (
	isDebugMode        = false
	channelDefaultSize = 100
)

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
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
	CommandIndex int
	Command      interface{}

	UseSnapshot bool
	Snapshot    []byte
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
	voteCount   int
	votedFor    int

	// data for 2B
	log         []LogEntry
	commitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh             chan ApplyMsg
	heartbeatReceivedCh chan bool
	voteGrantedCh       chan bool
	electedAsLeader     chan bool
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("Error during decoding information\n")
	} else {
		raft.currentTerm = currentTerm
		raft.votedFor = votedFor
		raft.log = log
	}
}

// ##################################################################################################
// ##################################################################################################
// #############################                                         ############################
// #############################                 Snapshot                ############################
// #############################                                         ############################
// ##################################################################################################
// ##################################################################################################

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// get previous encoded raft state size.
//
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// append raft information to kv server snapshot and save whole snapshot.
// the snapshot will include changes up to log entry with given index.
//
func (rf *Raft) CreateSnapshot(kvSnapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex, lastIndex := rf.log[0].Index, rf.getLastLogEntry(false).Index
	if index <= baseIndex || index > lastIndex {
		// can't trim log since index is invalid
		return
	}
	rf.truncateLog(index, rf.log[index-baseIndex].Term)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	snapshot := append(w.Bytes(), kvSnapshot...)

	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

//
// recover from previous raft snapshot.
//
func (rf *Raft) recoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.truncateLog(lastIncludedIndex, lastIncludedTerm)

	// send snapshot to kv server
	msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
	rf.applyCh <- msg
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.log[i-baseIndex].Command
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// reject requests with stale term number
		reply.Term = rf.currentTerm
		return
	}

	// if args.Term > rf.currentTerm {
	// 	// become follower and update current term
	// 	rf.role = Follower
	// 	rf.currentTerm = args.Term
	// 	rf.votedFor = -1
	// 	rf.persist()
	// }
	rf.tryIncreaseCurrentTerm(args.Term)

	// confirm heartbeat to refresh timeout
	rf.heartbeatReceivedCh <- true

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.truncateLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

		// send snapshot to kv server
		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.applyCh <- msg
	}
}

func (r *Raft) truncateLog(lastIndex int, lastTerm int) {
	zerothLogEntry := LogEntry{
		Index: lastIndex,
		Term:  lastTerm,
	}
	newLog := []LogEntry{zerothLogEntry}

	for i := len(r.log) - 1; i >= 0; i-- {
		if r.log[i].Index == lastIndex && r.log[i].Term == lastTerm {
			newLog = append(newLog, r.log[i+1:]...)
			break
		}
	}
	r.log = newLog
}

func (r *Raft) sendInstallSnapshotHandlerRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := r.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (r *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := r.sendInstallSnapshotHandlerRPC(server, args, reply)

	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()

	if r.killed() || !ok || r.role != Leader || args.Term != r.currentTerm {
		return false
	}

	if r.tryIncreaseCurrentTerm(reply.Term) {
		return true
	}

	r.nextIndex[server] = args.LastIncludedIndex + 1
	r.matchIndex[server] = args.LastIncludedIndex

	return true
}

// ##################################################################################################
// ##################################################################################################
// #############################                                         ############################
// #############################                RequestVote              ############################
// #############################                                         ############################
// ##################################################################################################
// ##################################################################################################

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
// example RequestVote RPC handler.
//
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		r.rejectRequestVote(args, reply)
	} else {
		r.tryIncreaseCurrentTerm(args.Term)

		if r.shouldAcceptRequestVote(args) {
			r.acceptVoteRequest(args, reply)
		} else {
			r.rejectRequestVote(args, reply)
		}
	}
}

func (r *Raft) shouldAcceptRequestVote(args *RequestVoteArgs) bool {
	return r.candidateLogIsUpToDate(args) && (r.votedFor == -1 || r.votedFor == args.CandidateID)
}

func (r *Raft) acceptVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	if isDebugMode {
		fmt.Printf("-- RequestVote: Raft#%v voted for Raft#%v\n", r.me, args.CandidateID)
		fmt.Printf("				Follower  Info: lastLogTerm: %v	lastLogIndex: %v\n", r.getLastLogEntry(false).Term, r.getLastLogEntry(false).Index)
		fmt.Printf("				Candidate Info: lastLogTerm: %v	lastLogIndex: %v\n", args.LastLogTerm, args.LastLogIndex)
	}
	reply.VoteGranted = true
	r.votedFor = args.CandidateID
	r.voteGrantedCh <- true
}

func (r *Raft) rejectRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if isDebugMode {
		fmt.Printf("-- RequestVote: Raft#%v rejected Raft#%v\n", r.me, args.CandidateID)
	}
	reply.VoteGranted = false
	reply.Term = r.currentTerm
}

func (r *Raft) candidateLogIsUpToDate(args *RequestVoteArgs) bool {
	lastLog := r.getLastLogEntry(false)
	if lastLog.Term < args.LastLogTerm {
		return true
	} else if lastLog.Term > args.LastLogTerm {
		return false
	} else { // lastLog.Term == lastLogTerm
		return lastLog.Index <= args.LastLogIndex
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

func (r *Raft) sendRequestVoteHandler(index int, args *RequestVoteArgs) {

	reply := &RequestVoteReply{}
	status := r.sendRequestVote(index, args, reply)

	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()

	if r.killed() ||
		status == false ||
		r.role != Candidate ||
		r.currentTerm != args.Term ||
		reply.Term > r.currentTerm ||
		!reply.VoteGranted {

		r.voteWasNotGranted(reply)
	} else {
		r.voteWasGranted()
	}
}

func (r *Raft) voteWasGranted() {
	r.voteCount++
	if r.voteCount > r.getServersCount()/2 {
		r.role = Leader
		if isDebugMode {
			fmt.Printf("-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- > Raft# %d became LEADER\n", r.me)
		}
		r.nextIndex = make([]int, len(r.peers))
		r.matchIndex = make([]int, len(r.peers))
		for i := range r.peers {
			r.nextIndex[i] = r.getLastLogEntry(false).Index + 1
		}

		r.electedAsLeader <- true
	}
}

func (r *Raft) voteWasNotGranted(reply *RequestVoteReply) {
	r.tryIncreaseCurrentTerm(reply.Term)
}

func (r *Raft) holdElection() {
	if isDebugMode {
		fmt.Printf("-- Raft #%d started Elections\n", r.me)
	}

	r.formalizeElectionStart()

	args := r.getRequestVoteArgs()

	for i := range r.peers {
		if !r.killed() && i != r.me && r.getRole(true) == Candidate {
			go r.sendRequestVoteHandler(i, args)
		}
	}
}

func (r *Raft) formalizeElectionStart() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()

	r.currentTerm++
	r.votedFor = r.me
	r.voteCount = 1
}

func (r *Raft) getRequestVoteArgs() *RequestVoteArgs {
	r.mu.Lock()
	defer r.mu.Unlock()

	return &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateID:  r.me,
		LastLogIndex: r.getLastLogEntry(false).Index,
		LastLogTerm:  r.getLastLogEntry(false).Term,
	}
}

// ##################################################################################################
// ##################################################################################################
// #############################                                         ############################
// #############################              Append Entries             ############################
// #############################                                         ############################
// ##################################################################################################
// ##################################################################################################

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term                  int
	Success               bool
	MismatchStartingIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// rf.tryIncreaseCurrentTerm(args.Term)
	// rf.initAppendEntriesReplyDefaults(reply)
	// if args.Term < rf.currentTerm {
	// 	return
	// }
	// rf.processAppendEntryRequest(args, reply)

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.MismatchStartingIndex = rf.getLastLogEntry(false).Index + 1
		return
	}

	rf.tryIncreaseCurrentTerm(args.Term)

	rf.heartbeatReceivedCh <- true

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogEntry(false).Index {
		reply.MismatchStartingIndex = rf.getLastLogEntry(false).Index + 1
		return
	}

	rf.rame(args, reply)
}

func (r *Raft) processAppendEntryRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.heartbeatReceivedCh <- true

	if args.PrevLogIndex <= r.getLastLogEntry(false).Index {
		if args.PrevLogTerm != r.log[args.PrevLogIndex].Term {
			r.rejectAppendEntriesRequest(args, reply)
		} else if args.PrevLogIndex >= 0 { // TODO -1
			r.acceptAppendEntriesRequest(args, reply)
		}
	}

	r.rame(args, reply)

}

func (r *Raft) initAppendEntriesReplyDefaults(reply *AppendEntriesReply) {
	reply.Success = false
	reply.Term = r.currentTerm
	reply.MismatchStartingIndex = r.getLastLogEntry(false).Index + 1
}

func (r *Raft) acceptAppendEntriesRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = true
	r.log = append(r.log[:args.PrevLogIndex+1], args.Entries...)
	reply.MismatchStartingIndex = args.PrevLogIndex + len(args.Entries)

	if r.commitIndex < args.LeaderCommit {
		r.commitIndex = min(args.LeaderCommit, r.getLastLogEntry(false).Index)
	}

}

func (r *Raft) rame(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	baseIndex := r.log[0].Index

	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != r.log[args.PrevLogIndex-baseIndex].Term {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict entries before.
		// bypass all entries during the problematic term to speed up.
		term := r.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if r.log[i-baseIndex].Term != term {
				reply.MismatchStartingIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		// otherwise log up to prevLogIndex are safe.
		// merge lcoal log and entries from leader, and apply log if commitIndex changes.
		r.log = r.log[:args.PrevLogIndex-baseIndex+1]
		r.log = append(r.log, args.Entries...)

		reply.Success = true
		reply.MismatchStartingIndex = args.PrevLogIndex + len(args.Entries)

		if r.commitIndex < args.LeaderCommit {
			// update commitIndex and apply log
			r.commitIndex = min(args.LeaderCommit, r.getLastLogEntry(false).Index)
			go r.applyLog()
		}
	}
}

func (r *Raft) rejectAppendEntriesRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	mismatchLogEntryTerm := r.log[args.PrevLogIndex].Term
	for i := args.PrevLogIndex - 1; i >= 0; i-- {
		if r.log[i].Term != mismatchLogEntryTerm {
			reply.MismatchStartingIndex = i + 1
			break
		}
	}
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (r *Raft) sendAppendEntriesHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.sendAppendEntries(server, args, reply)

	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()

	if r.killed() || !ok || r.role != Leader || args.Term != r.currentTerm {
		ok = false
	} else if reply.Term > r.currentTerm {
		r.increaseTermAndBecameFollower(reply.Term)
		ok = false
	} else if reply.Success {
		if len(args.Entries) > 0 {
			r.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
			r.nextIndex[server] = r.matchIndex[server] + 1
		}
	} else {
		r.nextIndex[server] = min(reply.MismatchStartingIndex, r.getLastLogEntry(false).Index)
	}

	if ok {
		r.updateCommitIndex()
	}

	return ok
}

func (r *Raft) updateCommitIndex() {
	baseIndex := r.log[0].Index
	for N := r.getLastLogEntry(false).Index; N > r.commitIndex && r.log[N-baseIndex].Term == r.currentTerm; N-- {
		// find if there exists an N to update commitIndex
		count := 1
		for i := range r.peers {
			if i != r.me && r.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(r.peers)/2 {
			r.commitIndex = N
			go r.applyLog()
			break
		}
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

func (r *Raft) broadcastHeartbeats() {
	r.mu.Lock()
	defer r.mu.Unlock()

	baseIndex := r.log[0].Index
	snapshot := r.persister.ReadSnapshot()

	for server := range r.peers {
		if !r.killed() && server != r.me && r.role == Leader {

			if r.nextIndex[server] > baseIndex {
				// args := r.getAppendEntriesArgs(server)
				reply := r.getAppendEntriesReply(server)

				args := &AppendEntriesArgs{}
				args.Term = r.currentTerm
				args.LeaderID = r.me
				args.PrevLogIndex = r.nextIndex[server] - 1
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = r.log[args.PrevLogIndex-baseIndex].Term
				}
				if r.nextIndex[server] <= r.getLastLogEntry(false).Index {
					args.Entries = r.log[r.nextIndex[server]-baseIndex:]
				}
				args.LeaderCommit = r.commitIndex

				go r.sendAppendEntriesHandler(server, args, reply)
			} else {
				args := &InstallSnapshotArgs{
					Term:              r.currentTerm,
					LeaderId:          r.me,
					LastIncludedIndex: r.log[0].Index,
					LastIncludedTerm:  r.log[0].Term,
					Data:              snapshot,
				}
				reply := &InstallSnapshotReply{}

				go r.sendInstallSnapshot(server, args, reply)
			}
		}
	}
}

func (r *Raft) getAppendEntriesArgs(server int) *AppendEntriesArgs {
	logIndex := r.nextIndex[server] - 1
	return &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.me,
		PrevLogIndex: logIndex,
		PrevLogTerm:  r.log[logIndex].Term,
		Entries:      r.log[r.nextIndex[server]:],
		LeaderCommit: r.commitIndex,
	}
}

func (r *Raft) getAppendEntriesReply(server int) *AppendEntriesReply {
	return &AppendEntriesReply{}
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
			Index:   r.getLastLogEntry(false).Index + 1,
			Term:    r.currentTerm,
			Command: command,
		}
		r.log = append(r.log, newLog)
		r.persist()
		if isDebugMode {
			fmt.Printf("-- Start: Raft#%d: log appended with %v\n", r.me, command)
		}
	}

	return r.getLastLogEntry(false).Index, r.getLastLogEntry(false).Term, r.role == Leader
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
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

	r.applyCh = applyCh
	r.heartbeatReceivedCh = make(chan bool, channelDefaultSize)
	r.voteGrantedCh = make(chan bool, channelDefaultSize)
	r.electedAsLeader = make(chan bool, channelDefaultSize)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())
	r.recoverFromSnapshot(persister.ReadSnapshot())
	r.persist()

	go r.Worker()
	// go r.applyLogWorker()

	return r
}

func (r *Raft) Worker() {
	for !r.killed() {
		switch r.role {
		case Follower:
			r.runFollowerJob()
		case Candidate:
			r.runCandidateJob()
		case Leader:
			r.runLeaderJob()
		}
	}
}

func (r *Raft) runLeaderJob() {
	go r.broadcastHeartbeats()
	time.Sleep(time.Millisecond * 60)
}

func (r *Raft) runCandidateJob() {
	go r.holdElection()

	select {
	case <-r.heartbeatReceivedCh:
		r.role = Follower
	case <-r.electedAsLeader: // Do nothing
	case <-time.After(r.getRandomFollowerWaitDuration()):
	}
}

func (r *Raft) runFollowerJob() {
	select {
	case <-r.voteGrantedCh: // Do nothing
	case <-r.heartbeatReceivedCh: // Do nothing
	case <-time.After(r.getRandomFollowerWaitDuration()):
		r.role = Candidate
		r.persist()
	}
}

func (r *Raft) applyLogWorker() {
	for !r.killed() {
		r.mu.Lock()
		for r.commitIndex > r.lastApplied {
			r.lastApplied++
			if len(r.log) <= r.lastApplied {
				break
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: r.lastApplied,
				Command:      r.log[r.lastApplied].Command,
			}
			r.applyCh <- applyMsg

			if isDebugMode {
				fmt.Printf("-- updateAppliedLogs: Raft#%d applied %v on index: %v\n", r.me, applyMsg.Command, applyMsg.CommandIndex)
			}
		}
		r.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

// ##################################################################################################
// ##################################################################################################
// #############################                                         ############################
// #############################  some small private methods about Raft  ############################
// #############################                                         ############################
// ##################################################################################################
// ##################################################################################################

func (r *Raft) getRandomFollowerWaitDuration() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(300)+200)
}

func (r *Raft) increaseTermAndBecameFollower(newTerm int) {
	r.currentTerm = newTerm
	r.votedFor = -1
	r.role = Follower
}

func (r *Raft) tryIncreaseCurrentTerm(termToCompare int) bool {
	if r.currentTerm < termToCompare {
		r.increaseTermAndBecameFollower(termToCompare)
		return true
	} else {
		return false
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

func (r *Raft) getLastLogEntry(mustBeSafe bool) LogEntry {
	if mustBeSafe {
		r.mu.Lock()
		defer r.mu.Unlock()
	}

	return r.log[len(r.log)-1]
}
