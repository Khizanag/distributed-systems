package raft

// //
// // this is an outline of the API that raft must expose to
// // the service (or tester). see comments below for
// // each of these functions for more details.
// //
// // rf = Make(...)
// //   create a new Raft server.
// // rf.Start(command interface{}) (index, term, isleader)
// //   start agreement on a new log entry
// // rf.GetState() (term, isLeader)
// //   ask a Raft for its current term, and whether it thinks it is leader
// // ApplyMsg
// //   each time a new entry is committed to the log, each Raft peer
// //   should send an ApplyMsg to the service (or tester)
// //   in the same server.
// //

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
)

// // import "bytes"
// // import "../labgob"

// // enum that concretes if server is Leader, Candidate or Followers
// type Role int

// const (
// 	Leader Role = iota
// 	Candidate
// 	Follower
// )

// const (
// 	MinWaitMSsForRequest = 300
// 	MaxWaitMSsForRequest = 600
// 	SleepMSsForLeader    = 110 // max 10 in 1 sec
// 	SleepMSsForCandidate = 200
// 	SleepMSsForFollower  = 60

// 	MaxWaitMSsForElections              = 250
// 	SleepMSsForCandidateDuringElections = 20
// )

// const (
// 	isDebugMode = false
// )

// //
// // as each Raft peer becomes aware that successive log entries are
// // committed, the peer should send an ApplyMsg to the service (or
// // tester) on the same server, via the applyCh passed to Make(). set
// // CommandValid to true to indicate that the ApplyMsg contains a newly
// // committed log entry.
// //
// // in Lab 3 you'll want to send other kinds of messages (e.g.,
// // snapshots) on the applyCh; at that point you can add fields to
// // ApplyMsg, but set CommandValid to false for these other uses.
// //
// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// 	UseSnapshot  bool
// 	Snapshot     []byte
// }

// type Log struct {
// 	Index   int
// 	Term    int
// 	Command interface{}
// }

// //
// // A Go object implementing a single Raft peer.
// //
// type Raft struct {
// 	mu        sync.Mutex          // Lock to protect shared access to this peer's state
// 	peers     []*labrpc.ClientEnd // RPC end points of all peers
// 	persister *Persister          // Object to hold this peer's persisted state
// 	me        int                 // this peer's index into peers[]
// 	dead      int32               // set by Kill()

// 	// Your data here (2A, 2B, 2C).
// 	// Look at the paper's Figure 2 for a description of what
// 	// state a Raft server must maintain.
// 	role                Role // role of this server: follower, candidate or leader
// 	currentTerm         int
// 	votedFor            int
// 	electionRequestTime time.Time // time at which last heartbeat was received

// 	// data for 2B
// 	logs        []Log
// 	commitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
// 	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
// 	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
// 	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

// 	applyCh chan ApplyMsg
// }

// // return currentTerm and whether this server
// // believes it is the leader.
// func (r *Raft) GetState() (int, bool) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	return r.currentTerm, r.role == Leader
// }

// //
// // save Raft's persistent state to stable storage,
// // where it can later be retrieved after a crash and restart.
// // see paper's Figure 2 for a description of what should be persistent.
// //
// func (r *Raft) persist() {
// 	w := new(bytes.Buffer)
// 	e := gob.NewEncoder(w)
// 	e.Encode(r.dead)
// 	e.Encode(r.currentTerm)
// 	e.Encode(r.votedFor)
// 	e.Encode(r.logs)
// 	data := w.Bytes()
// 	r.persister.SaveRaftState(data)
// }

// //
// // restore previously persisted state.
// //
// func (raft *Raft) readPersist(data []byte) {
// 	if data == nil || len(data) < 1 { // bootstrap without any state?
// 		return
// 	}

// 	r := bytes.NewBuffer(data)
// 	d := gob.NewDecoder(r)

// 	var dead int32
// 	var currentTerm int
// 	var votedFor int
// 	var logs []Log

// 	if d.Decode(&dead) != nil ||
// 		d.Decode(&currentTerm) != nil ||
// 		d.Decode(&votedFor) != nil ||
// 		d.Decode(&logs) != nil {
// 		fmt.Printf("Error during decoding information\n")
// 	} else {
// 		raft.dead = dead
// 		raft.currentTerm = currentTerm
// 		raft.votedFor = votedFor
// 		raft.logs = logs
// 	}
// }

// type AppendEntriesArgs struct {
// 	Term         int   // leader's term
// 	LeaderID     int   // so follower can redirect clients
// 	PrevLogIndex int   // index of log entry immediately preceding new ones
// 	PrevLogTerm  int   // term of prevLogIndex entry
// 	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
// 	LeaderCommit int   // leader's commitIndex
// }

// type AppendEntriesReply struct {
// 	Term          int
// 	Success       bool
// 	MismatchIndex int
// }

// func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	defer r.persist()

// 	r.updateElectionRequestTime()

// 	if args.Term < r.currentTerm || args.LeaderID == r.me { // TODO remove leader check and uncomment block
// 		if isDebugMode {
// 			fmt.Printf("-- AppendEntries: Raft#%v Didn't Appended Entries from %v -> Reason: args.Term(%v) < r.currentTerm(%v)\n", r.me, args.LeaderID, args.Term, r.currentTerm)
// 		}
// 		r.tryUpdateCurrentTerm(args.Term)
// 		reply.Term = r.currentTerm
// 		reply.Success = false

// 	} else if args.PrevLogIndex < 0 {
// 		r.acceptAppendEntries(args, reply)
// 		if isDebugMode {
// 			fmt.Printf("-- AppendEntries: Raft#%v Appended Entries: %v from %v, Reason: args.PrevLogIndex < 0\n", r.me, args.Entries, args.LeaderID)
// 		}

// 	} else if len(r.logs) <= args.PrevLogIndex {
// 		if isDebugMode {
// 			fmt.Printf("-- AppendEntries: Raft#%v Didn't Appended Entries from %v -> Reason: len(r.logs) <= args.PrevLogIndex\n", r.me, args.LeaderID)
// 		}
// 		r.tryUpdateCurrentTerm(args.Term)
// 		reply.Term = r.currentTerm
// 		reply.Success = false
// 		reply.MismatchIndex = len(r.logs)

// 	} else if r.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
// 		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 		if isDebugMode {
// 			fmt.Printf("-- AppendEntries: Raft#%v Didn't Appended Entries from %v -> Reason: r.logs[args.PrevLogIndex].Term != args.PrevLogTerm\n", r.me, args.LeaderID)
// 		}
// 		r.tryUpdateCurrentTerm(args.Term)
// 		reply.Term = r.currentTerm
// 		reply.Success = false

// 		for i := args.PrevLogIndex; i > 0; i-- {
// 			if r.logs[i].Term != r.logs[args.PrevLogIndex].Term {
// 				reply.MismatchIndex = i + 1
// 				break
// 			}
// 		}
// 		reply.MismatchIndex = 1

// 	} else { // receive RPC request
// 		r.acceptAppendEntries(args, reply)
// 		if isDebugMode {
// 			fmt.Printf("-- AppendEntries: Raft#%v Appended Entries: %v from %v\n", r.me, args.Entries, args.LeaderID)
// 		}
// 	}
// }

// func (r *Raft) tryUpdateCurrentTerm(termToCompare int) {
// 	if r.currentTerm < termToCompare {
// 		r.currentTerm = termToCompare
// 		r.votedFor = -1
// 		r.role = Follower
// 		r.persist()
// 	}
// }

// func (r *Raft) acceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	reply.Success = true

// 	r.role = Follower
// 	if r.currentTerm < args.Term {
// 		r.currentTerm = args.Term
// 		r.votedFor = -1
// 	}

// 	if len(args.Entries) > 0 {
// 		r.logs = append(r.logs[:args.PrevLogIndex+1], args.Entries...)
// 	}

// 	if args.LeaderCommit > r.commitIndex {
// 		r.commitIndex = intMin(args.LeaderCommit, r.getLastLog().Index)
// 	}
// }

// func intMin(a int, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }

// func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

// func (r *Raft) sendAppendEntriesHandler(index int, numReceivedBeats *int32) {
// 	// doing this in loop, because we are waiting for another server to receive out infos
// 	for {
// 		r.mu.Lock()
// 		if r.killed() || r.role != Leader {
// 			r.mu.Unlock()
// 			break
// 		}

// 		logIndex := r.nextIndex[index] - 1

// 		args := AppendEntriesArgs{
// 			Term:         r.currentTerm,
// 			LeaderID:     r.me,
// 			PrevLogIndex: logIndex,
// 			PrevLogTerm:  r.logs[logIndex].Term,
// 			Entries:      r.logs[r.nextIndex[index]:],
// 			LeaderCommit: r.commitIndex,
// 		}
// 		r.mu.Unlock()

// 		reply := AppendEntriesReply{}
// 		status := r.sendAppendEntries(index, &args, &reply)

// 		r.mu.Lock()
// 		if status {
// 			if reply.Success {
// 				r.matchIndex[index] = r.getLastLog().Index
// 				r.nextIndex[index] = r.matchIndex[index] + 1
// 				atomic.AddInt32(numReceivedBeats, 1)
// 				r.mu.Unlock()
// 				break // this goroutines job is done
// 			} else {
// 				if reply.Term > r.currentTerm {
// 					r.currentTerm = reply.Term
// 					r.role = Follower
// 					if isDebugMode {
// 						fmt.Printf("-- Raft#%v became FOLLOWER\n", r.me)
// 					}
// 					r.votedFor = -1
// 					r.persist()
// 					r.mu.Unlock()
// 					break // this goroutines job is done
// 				} else {
// 					r.nextIndex[index] = reply.MismatchIndex
// 				}
// 			}
// 		}
// 		r.mu.Unlock()
// 	}
// }

// //
// // example RequestVote RPC arguments structure.
// // field names must start with capital letters!
// //
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int // candidate's term
// 	CandidateID  int // candidate requesting vote
// 	LastLogIndex int // index of candidate's last log entry
// 	LastLogTerm  int // term of candidate's last log entry

// 	CommitIndex int
// }

// //
// // RequestVote RPC reply structure.
// // field names must start with capital letters!
// //
// type RequestVoteReply struct {
// 	Term        int  // currentTerm, for candidate to update itself
// 	VoteGranted bool // true means candidate received vote
// }

// //
// // RequestVote RPC handler.
// //
// func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	// Your code here (2A, 2B).
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	defer r.persist()

// 	if args.LastLogIndex > r.getLastLog().Index && r.votedFor == r.me {
// 		if isDebugMode {
// 			fmt.Printf("-- RequestVote: Raft#%v voted for Raft#%v(CustomVote)\n", r.me, args.CandidateID)
// 		}
// 		r.acceptVoteRequest(args, reply)
// 	} else if r.candidateLogIsUpToDate(args) &&
// 		(r.currentTerm < args.Term ||
// 			(r.currentTerm == args.Term &&
// 				(r.votedFor == -1 || r.votedFor == args.CandidateID))) { // TODO check for errors
// 		// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
// 		if isDebugMode {
// 			fmt.Printf("-- RequestVote: Raft#%v voted for Raft#%v\n", r.me, args.CandidateID)
// 			fmt.Printf("				Follower  Info: lastLogTerm: %v	lastLogIndex: %v\n", r.getLastLog().Term, r.getLastLog().Index)
// 			fmt.Printf("				Candidate Info: lastLogTerm: %v	lastLogIndex: %v\n", args.LastLogTerm, args.LastLogIndex)
// 		}
// 		r.acceptVoteRequest(args, reply)
// 	} else {
// 		if isDebugMode {
// 			fmt.Printf("-- RequestVote: Raft#%v rejected Raft#%v\n", r.me, args.CandidateID)
// 		}
// 		r.rejectRequestVote(args, reply)
// 	}
// }

// func (r *Raft) acceptVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	reply.VoteGranted = true

// 	r.currentTerm = args.Term
// 	r.votedFor = args.CandidateID
// 	r.role = Follower
// 	r.updateElectionRequestTime()
// }

// func (r *Raft) rejectRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	reply.VoteGranted = false

// 	if args.Term > r.currentTerm {
// 		r.currentTerm = args.Term
// 		r.votedFor = -1
// 	}
// 	reply.Term = r.currentTerm
// }

// func (r *Raft) candidateLogIsUpToDate(args *RequestVoteArgs) bool {
// 	if r.commitIndex > args.CommitIndex {
// 		return false
// 	}

// 	lastLog := r.getLastLog()
// 	if lastLog.Term < args.LastLogTerm {
// 		return true
// 	} else if lastLog.Term > args.LastLogTerm {
// 		return false
// 	} else { // lastLog.Term == lastLogTerm
// 		return lastLog.Index <= args.LastLogIndex
// 	}
// }

// //
// // example code to send a RequestVote RPC to a server.
// // server is the index of the target server in rf.peers[].
// // expects RPC arguments in args.
// // fills in *reply with RPC reply, so caller should
// // pass &reply.
// // the types of the args and reply passed to Call() must be
// // the same as the types of the arguments declared in the
// // handler function (including whether they are pointers).
// //
// // The labrpc package simulates a lossy network, in which servers
// // may be unreachable, and in which requests and replies may be lost.
// // Call() sends a request and waits for a reply. If a reply arrives
// // within a timeout interval, Call() returns true; otherwise
// // Call() returns false. Thus Call() may not return for a while.
// // A false return can be caused by a dead server, a live server that
// // can't be reached, a lost request, or a lost reply.
// //
// // Call() is guaranteed to return (perhaps after a delay) *except* if the
// // handler function on the server side does not return.  Thus there
// // is no need to implement your own timeouts around Call().
// //
// // look at the comments in ../labrpc/labrpc.go for more details.
// //
// // if you're having trouble getting RPC to work, check that you've
// // capitalized all field names in structs passed over RPC, and
// // that the caller passes the address of the reply struct with &, not
// // the struct itself.
// //
// func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// func (r *Raft) sendRequestVoteHandler(index int, newTerm int, numAccepts *int32, numRejects *int32) {
// 	if r.killed() || r.getRoleSafe() != Candidate {
// 		return
// 	}

// 	args := RequestVoteArgs{
// 		Term:         newTerm,
// 		CandidateID:  r.me,
// 		LastLogIndex: r.getLastLogSafe().Index,
// 		LastLogTerm:  r.getLastLogSafe().Term,
// 		CommitIndex:  r.commitIndex,
// 	}
// 	reply := RequestVoteReply{}

// 	r.sendRequestVote(index, &args, &reply)

// 	if reply.VoteGranted {
// 		atomic.AddInt32(numAccepts, 1)
// 	} else {
// 		atomic.AddInt32(numRejects, 1)

// 		r.mu.Lock()

// 		if reply.Term > newTerm {
// 			r.currentTerm = reply.Term
// 			r.votedFor = -1
// 			r.persist()
// 			r.updateElectionRequestTime()
// 			r.role = Follower
// 		}

// 		r.mu.Unlock()
// 	}
// }

// func (r *Raft) watcher() {
// 	for !r.killed() {
// 		role := r.getRoleSafe()
// 		switch role {
// 		case Leader:
// 			go r.broadcastHeartBeats()
// 			time.Sleep(SleepMSsForLeader * time.Millisecond)
// 		case Candidate:
// 			r.startElections()
// 		case Follower:
// 			time.Sleep(SleepMSsForFollower * time.Millisecond)
// 			r.tryBecomingCandidate()
// 		}
// 	}
// }

// func (r *Raft) updateAppliedLogsMonitor() {
// 	for !r.killed() {
// 		r.mu.Lock()
// 		for r.commitIndex > r.lastApplied {
// 			r.lastApplied++
// 			if len(r.logs) <= r.lastApplied {
// 				break
// 			}
// 			applyMsg := ApplyMsg{
// 				CommandValid: true,
// 				CommandIndex: r.lastApplied,
// 				Command:      r.logs[r.lastApplied].Command,
// 			}
// 			r.applyCh <- applyMsg

// 			if isDebugMode {
// 				fmt.Printf("-- updateAppliedLogs: Raft#%d applied %v on index: %v\n", r.me, applyMsg.Command, applyMsg.CommandIndex)
// 			}
// 		}
// 		r.mu.Unlock()

// 		time.Sleep(20 * time.Millisecond)
// 	}

// }

// func (r *Raft) startElections() {
// 	r.mu.Lock()

// 	if isDebugMode {
// 		fmt.Printf("-- Raft #%d started Elections with: currentTerm: %v		commitIndex: %v		logLen: %v\n", r.me, r.currentTerm, r.commitIndex, len(r.logs))
// 	}

// 	r.currentTerm++
// 	r.updateElectionRequestTime()
// 	newTerm := r.currentTerm
// 	var numAccepts int32 = 1 // self vote
// 	r.votedFor = r.me
// 	var numRejects int32 = 0
// 	r.persist()
// 	r.mu.Unlock()

// 	for i := range r.peers {
// 		if i == r.me {
// 			continue
// 		}

// 		if r.killed() || r.getRoleSafe() != Candidate {
// 			return
// 		}
// 		go r.sendRequestVoteHandler(i, newTerm, &numAccepts, &numRejects)
// 	}

// 	r.ceskoFn(&numAccepts, &numRejects)
// }

// func (r *Raft) ceskoFn(numAccepts *int32, numRejects *int32) {
// 	var numServers int32 = int32(r.getServersCount())
// 	waitUntil := time.Now().Add(time.Duration(MaxWaitMSsForElections) * time.Millisecond)

// 	for { // wait for other servers

// 		time.Sleep(10 * time.Millisecond)

// 		if r.dead == 1 || r.getRoleSafe() != Candidate {
// 			return
// 		} else if atomic.LoadInt32(numAccepts) > numServers/2 { // became leader
// 			r.mu.Lock()
// 			r.role = Leader

// 			if isDebugMode {
// 				fmt.Printf("-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- > Raft# %d became LEADER\n", r.me)
// 			}
// 			r.nextIndex = make([]int, len(r.peers))
// 			r.matchIndex = make([]int, len(r.peers))
// 			for i := range r.peers {
// 				r.nextIndex[i] = r.getLastLog().Index + 1
// 			}
// 			r.mu.Unlock()
// 			go r.broadcastHeartBeats()
// 			break
// 		} else if atomic.LoadInt32(numRejects) >= numServers/2 { // wasn't chosen as Leader
// 			r.mu.Lock()
// 			r.role = Follower
// 			r.mu.Unlock()
// 			break
// 		} else if time.Now().After(waitUntil) { // time is overs
// 			break
// 		}
// 	}
// }

// // requires raft's mutex locking
// func (r *Raft) broadcastHeartBeats() {

// 	var numReceivedBeats int32 = 0

// 	for i := range r.peers {

// 		if i == r.me { // use of r.me without lock, no race condition, because me is not changing
// 			continue
// 		}
// 		go r.sendAppendEntriesHandler(i, &numReceivedBeats)
// 	}

// 	time.Sleep(50 * time.Millisecond) // wait until appendEntries are processed

// 	if atomic.LoadInt32(&numReceivedBeats) == 0 {
// 		r.mu.Lock()
// 		r.role = Follower
// 		r.mu.Unlock()
// 	} else {
// 		r.updateCommitIndexes(true)
// 	}
// }

// func (r *Raft) updateCommitIndexes(mustBeSafe bool) {
// 	if mustBeSafe {
// 		r.mu.Lock()
// 		defer r.mu.Unlock()
// 	}

// 	for i := r.commitIndex + 1; i < len(r.logs); i++ {

// 		if r.countServersThatReceived(i) > r.getServersCount()/2 {
// 			r.commitIndex = i
// 		} else {
// 			break
// 		}
// 	}
// }

// func (r *Raft) countServersThatReceived(i int) int {
// 	count := 1
// 	for ip := range r.peers {
// 		if ip == r.me {
// 			continue
// 		}
// 		if r.matchIndex[ip] >= i {
// 			count++
// 		}
// 	}
// 	return count
// }

// func (r *Raft) tryBecomingCandidate() {
// 	r.mu.Lock()
// 	electionRequestTime := r.electionRequestTime
// 	r.mu.Unlock()
// 	if time.Now().After(electionRequestTime) { // heartbeat should already be received
// 		r.role = Candidate
// 	}
// }

// //
// // the service using Raft (e.g. a k/v server) wants to start
// // agreement on the next command to be appended to Raft's log. if this
// // server isn't the leader, returns false. otherwise start the
// // agreement and return immediately. there is no guarantee that this
// // command will ever be committed to the Raft log, since the leader
// // may fail or lose an election. even if the Raft instance has been killed,
// // this function should return gracefully.
// //
// // the first return value is the index that the command will appear at
// // if it's ever committed. the second return value is the current
// // term. the third return value is true if this server believes it is
// // the leader.
// //
// func (r *Raft) Start(command interface{}) (int, int, bool) {
// 	// Your code here (2B).

// 	r.mu.Lock()
// 	defer r.mu.Unlock()

// 	if r.role == Leader {
// 		newLog := Log{
// 			Index:   len(r.logs),
// 			Term:    r.currentTerm,
// 			Command: command,
// 		}
// 		r.logs = append(r.logs, newLog)
// 		r.persist()
// 		if isDebugMode {
// 			fmt.Printf("-- Start: Raft#%d: log appended with %v\n", r.me, command)
// 		}
// 	}

// 	return len(r.logs) - 1, r.getLastLog().Term, r.role == Leader
// }

// //
// // the tester doesn't halt goroutines created by Raft after each test,
// // but it does call the Kill() method. your code can use killed() to
// // check whether Kill() has been called. the use of atomic avoids the
// // need for a lock.
// //
// // the issue is that long-running goroutines use memory and may chew
// // up CPU time, perhaps causing later tests to fail and generating
// // confusing debug output. any goroutine with a long-running loop
// // should call killed() to check whether it should stop.
// //
// func (r *Raft) Kill() {
// 	atomic.StoreInt32(&r.dead, 1)
// 	// Your code here, if desired.
// 	r.persist()
// }

// func (r *Raft) killed() bool {
// 	z := atomic.LoadInt32(&r.dead)
// 	return z == 1
// }

// //
// // the service or tester wants to create a Raft server. the ports
// // of all the Raft servers (including this one) are in peers[]. this
// // server's port is peers[me]. all the servers' peers[] arrays
// // have the same order. persister is a place for this server to
// // save its persistent state, and also initially holds the most
// // recent saved state, if any. applyCh is a channel on which the
// // tester or service expects Raft to send ApplyMsg messages.
// // Make() must return quickly, so it should start goroutines
// // for any long-running work.
// //
// func Make(peers []*labrpc.ClientEnd, me int,
// 	persister *Persister, applyCh chan ApplyMsg) *Raft {
// 	r := &Raft{}
// 	r.peers = peers
// 	r.persister = persister
// 	r.me = me
// 	r.dead = 0

// 	// Your initialization code here 2A
// 	r.role = Follower
// 	r.currentTerm = 0
// 	r.votedFor = -1
// 	r.updateElectionRequestTime()

// 	// Your initialization code here 2B, 2C).
// 	r.commitIndex = 0
// 	r.lastApplied = 0
// 	zerothLog := Log{
// 		Index:   0,
// 		Term:    r.currentTerm,
// 		Command: nil,
// 	}
// 	r.logs = []Log{zerothLog}

// 	r.applyCh = applyCh

// 	go r.watcher()
// 	go r.updateAppliedLogsMonitor()

// 	// initialize from state persisted before a crash
// 	r.readPersist(persister.ReadRaftState())
// 	return r
// }

// // ##############################################################################
// // ##############################################################################
// // ###################                                         ##################
// // ###################  some small private methods about Raft  ##################
// // ###################                                         ##################
// // ##############################################################################
// // ##############################################################################

// func (r *Raft) printInfo(shouldBeSafe bool, printLogs bool, printNextIndexes bool, printMatchIndexes bool) {
// 	if shouldBeSafe {
// 		r.mu.Lock()
// 		defer r.mu.Unlock()
// 	}

// 	r.printMainInfo()

// 	if printLogs {
// 		r.printLogs()
// 	}

// 	if printNextIndexes {
// 		r.printNextIndexes()
// 	}

// 	if printMatchIndexes {
// 		r.printMatchIndexes()
// 	}
// }

// func (r *Raft) printMainInfo() {
// 	fmt.Printf("\n--------------------------------------Raft#%v----------------------------------------\n", r.me)
// 	fmt.Printf("		role: %v\n", r.role)
// 	fmt.Printf("		currentTerm	: %v\n", r.currentTerm)
// 	fmt.Printf("		votedFor	: %v\n", r.votedFor)
// 	fmt.Printf("		lastLogTerm	: %v\n", r.getLastLog().Term)
// 	fmt.Printf("		lastLogIndex: %v\n", r.getLastLog().Index)
// 	fmt.Printf("		logLen		: %v\n", len(r.logs))
// 	fmt.Printf("		commitIndex	: %v\n", r.commitIndex)
// 	fmt.Printf("		lastApplied	: %v\n", r.lastApplied)
// }

// func (r *Raft) printLogs() {
// 	for iLog := range r.logs {
// 		log := r.logs[iLog]
// 		fmt.Printf("Log #%v: Term: %v, Command: %v\n", log.Index, log.Term, log.Command)
// 	}
// }

// func (r *Raft) printNextIndexes() {
// 	fmt.Printf("		nextIndexes	: ")
// 	for i := range r.peers {
// 		fmt.Printf("%v ", r.nextIndex[i])
// 	}
// 	fmt.Printf("\n")
// }

// func (r *Raft) printMatchIndexes() {
// 	fmt.Printf("		matchIndexes: ")
// 	for i := range r.peers {
// 		fmt.Printf("%v ", r.matchIndex[i])
// 	}
// 	fmt.Printf("\n")
// }

// func (r *Raft) getServersCount() int {
// 	return len(r.peers)
// }

// func (r *Raft) getRoleSafe() Role {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	return r.getRole()
// }

// func (r *Raft) getRole() Role {
// 	return r.role
// }

// func (r *Raft) updateElectionRequestTimeSafe() {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	r.updateElectionRequestTime()
// }

// func (r *Raft) updateElectionRequestTime() {
// 	numMilliSeconds := rand.Intn(MaxWaitMSsForRequest-MinWaitMSsForRequest) + MinWaitMSsForRequest
// 	r.electionRequestTime = time.Now().Add(time.Duration(numMilliSeconds) * time.Millisecond)
// }

// func (r *Raft) getLastLogSafe() Log {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()

// 	return r.getLastLog()
// }

// func (r *Raft) getLastLog() Log {
// 	return r.logs[len(r.logs)-1]
// }

// // To test type this in terminal:
// //		go test -run 2A
// //		go test -run 2B
// //		go test -run 2C

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

// import "bytes"
// import "labgob"

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// Raft server states.
//
const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     int
	voteCount int

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Channels between raft peers.
	chanApply     chan ApplyMsg
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == STATE_LEADER)
	return term, isleader
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// encode current raft state.
//
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

	baseIndex, lastIndex := rf.log[0].Index, rf.getLastLogIndex()
	if index <= baseIndex || index > lastIndex {
		// can't trim log since index is invalid
		return
	}
	rf.trimLog(index, rf.log[index-baseIndex].Term)

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
	rf.trimLog(lastIncludedIndex, lastIncludedTerm)

	// send snapshot to kv server
	msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
	rf.chanApply <- msg
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
		rf.state = STATE_FOLLOWER
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
		if rf.state != STATE_CANDIDATE || rf.currentTerm != args.Term {
			// invalid request
			return ok
		}
		if rf.currentTerm < reply.Term {
			// revert to follower state and update current term
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				// win the election
				rf.state = STATE_LEADER
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
		if server != rf.me && rf.state == STATE_CANDIDATE {
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
		rf.state = STATE_FOLLOWER
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

	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}
	if reply.Term > rf.currentTerm {
		// become follower and update current term
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
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

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// reject requests with stale term number
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// become follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// confirm heartbeat to refresh timeout
	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

		// send snapshot to kv server
		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.chanApply <- msg
	}
}

//
// discard old log entries up to lastIncludedIndex.
//
func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) {
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}

	if reply.Term > rf.currentTerm {
		// become follower and update current term
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
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
	snapshot := rf.persister.ReadSnapshot()

	for server := range rf.peers {
		if server != rf.me && rf.state == STATE_LEADER {
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
			} else {
				args := &InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].Index
				args.LastIncludedTerm = rf.log[0].Term
				args.Data = snapshot

				go rf.sendInstallSnapshot(server, args, &InstallSnapshotReply{})
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, index := -1, -1
	isLeader := (rf.state == STATE_LEADER)

	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Run() {
	for {
		switch rf.state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.state = STATE_CANDIDATE
				rf.persist()
			}
		case STATE_LEADER:
			go rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * 60)
		case STATE_CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				rf.state = STATE_FOLLOWER
			case <-rf.chanWinElect:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
			}
		}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnapshot(persister.ReadSnapshot())
	rf.persist()

	go rf.Run()

	return rf
}
