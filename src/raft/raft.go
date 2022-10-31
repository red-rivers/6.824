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
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

var (
	Leader Role = 1
	Candidate Role = 2
	Follower Role = 3
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command  interface{}
	Term     int
	Index    int
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

	currentTerm int
	voteFor     int
	log         []Log

	applyCh     chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   map[int]int
	matchIndex  map[int]int

	role        Role
	refreshTicker bool

	lastIncludedIndex int
	lastIncludedTerm int
	snapShot         []byte
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.role==Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
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
	if d.Decode(&rf.currentTerm) != nil ||
	   d.Decode(&rf.voteFor) != nil || 
	   d.Decode(&rf.log) != nil {
	  DPrintf("readPersist err,  data : %+v", data)
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.persist()
	}
	DPrintf("[server %d] RequestVote args : %+v, voteFor : %+v", rf.me ,args, rf.voteFor)

	if (args.LastLogTerm > rf.getLastLog().Term || (args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex >= rf.getLastLog().Index)) && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		rf.role = Follower
		rf.voteFor = args.CandidateId
		rf.refreshTicker = true
		reply.VoteGranted = true
		rf.persist()
	}
	DPrintf("[server %d] RequestVote reply : %+v",rf.me, reply)
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries  []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	ConfilctTerm int
	ConfilctIndex int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("[server %d] AppendEntries args : %+v, reply : %+v",rf.me, args, reply)
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.role = Follower
	rf.refreshTicker = true

	success, log := rf.isLogMatch(args.PrevLogIndex, args.PrevLogTerm)
	if success {
		rf.log = rf.appendLog(args.PrevLogIndex, rf.log, args.Entries)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(rf.getLastLog().Index, args.LeaderCommit)
		} 
		reply.Success = true
		rf.persist()
	}else {
		if log != nil{
			reply.ConfilctTerm = log.Term
			i := args.PrevLogIndex-1
			for; i>=0 && rf.log[i].Term == reply.ConfilctTerm; i--{}
			reply.ConfilctIndex = rf.log[i+1].Index
			reply.Success = false
			rf.persist()
		}else {
			reply.ConfilctIndex = rf.getLastLog().Index+1
			reply.Success = false
		}
	}
	DPrintf("[server %d] AppendEntries reply : %+v, log: %+v",rf.me, reply, rf.log)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartsBeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me{
			continue
		}
		info := rf.StructureAE(peer)
		go func (peer int, info AppendEntriesArgs)  {
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peer, &info, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.voteFor = -1
					rf.persist()
				}
				if rf.role == Leader && info.Term == rf.currentTerm{
					if reply.Success {
						rf.matchIndex[peer] = info.PrevLogIndex+len(info.Entries)
						rf.nextIndex[peer] = rf.matchIndex[peer]+1
					}else{
						rf.fallback(peer, info.PrevLogIndex, reply.ConfilctIndex, reply.ConfilctTerm)
					}
				}
			}
		}(peer, info)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
	var voted int32 = 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: rf.getLastLog().Index,
			LastLogTerm: rf.getLastLog().Term,
		}

		go func (peer int, voted *int32, info RequestVoteArgs)  {
			args := &RequestVoteArgs{
				Term: info.Term,
				CandidateId: info.CandidateId,
				LastLogIndex: info.LastLogIndex,
				LastLogTerm: info.LastLogTerm,
			}
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(peer, args, reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.role = Follower
					rf.persist()
				}
				if reply.VoteGranted && rf.role == Candidate && args.Term == rf.currentTerm {
					atomic.AddInt32(voted, 1)
					if *voted > int32(len(rf.peers)/2) {
						rf.role = Leader
						for peer := range rf.peers {
							rf.nextIndex[peer] = rf.getLastLog().Index+1
						}
						go rf.sendHeartsBeats()
						DPrintf("[leader %d] win electioni", rf.me)
					}
				}
			}
		}(peer, &voted, args)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == Leader
	if !isLeader {
		return -1, -1, isLeader
	}
	index := rf.getLastLog().Index+1
	term := rf.currentTerm
	log := Log{
		Command: command,
		Term: rf.currentTerm,
		Index: index,
	}
	rf.log = append(rf.log, log)
	DPrintf("[leader %d] recive command : %+v", rf.me, log)
	rf.persist()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(time.Duration(rf.electionTimeOut())*time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}
		if rf.refreshTicker {
			rf.refreshTicker = false
			rf.mu.Unlock()
			continue
		}
		DPrintf("[candidate %d] start new election", rf.me)
		rf.role = Candidate
		go rf.startElection()
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartsBeats() {
	for !rf.killed() {
		time.Sleep(time.Duration(100)*time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			go rf.sendHeartsBeats()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionTimeOut() int {
	randonTime := 150 + rand.Int63()%150
	return int(randonTime)
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
	rf.log = make([]Log, 0)
	rf.voteFor = -1
	rf.role = Follower
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.getLastLog().Index+1
	}
	for peer := range rf.peers {
		rf.matchIndex[peer] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartsBeats()
	go rf.checkCommit()
	go rf.applyLog()


	return rf
}

func (rf *Raft) getLastLog() Log {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	}
	return Log{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
}

func (rf *Raft) appendLog(prevIndex int, source []Log, target []Log) []Log{
	res := make([]Log, 0)
	tmp := make([]Log, 0)
	for i:=0; i<len(rf.log); i++{
		log := rf.log[i]
		if log.Index <= prevIndex {
			res = append(res, log)
		}else{	
			tmp = append(tmp, log)
		}
	}
	for i:=0; i<len(tmp); i++{
		if len(target) > i && tmp[i].Term != target[i].Term {
			tmp = tmp[:i]	
		}
	}
	if len(tmp) > len(target) {
		res = append(res, tmp...)
	}else{
		res = append(res, target...)
	}
	return res
}

func (rf *Raft) isLogMatch(prevLogIndex, prevLogTerm int) (bool, *Log) {
	if prevLogIndex == rf.lastIncludedIndex && prevLogTerm == rf.lastIncludedTerm {
		return true, nil
	}
	var matchLog *Log
	for i:=0; i<len(rf.log); i++{
		log := rf.log[i]
		if log.Index == prevLogIndex {
			matchLog = &log
		}
	}
	if matchLog == nil {
		return false, nil
	}
	if matchLog.Term != prevLogTerm {
		return false, matchLog
	}
	return true, matchLog
}

func (rf *Raft) StructureAE(peer int) (Ae AppendEntriesArgs) {
	nextIdx := rf.nextIndex[peer]
	var prevLog Log
	entries := make([]Log, 0)

	for i:=0; i<len(rf.log); i++{
		if rf.log[i].Index == nextIdx-1 {
			prevLog = rf.log[i]
		}
		if rf.log[i].Index >= nextIdx {
			entries = append(entries, rf.log[i])
		}
	}
	Ae.Entries = entries
	Ae.PrevLogIndex = prevLog.Index
	Ae.PrevLogTerm = prevLog.Term
	Ae.LeaderCommit = rf.commitIndex
	Ae.LeaderId = rf.me
	Ae.Term = rf.currentTerm

	DPrintf("[leader %d to server %d] AppendEntries args : %+v, nextIdx is %+v",rf.me, peer, Ae, nextIdx)

	return Ae
}

func (rf *Raft) fallback(peer, prevLogIndex, confictIndex, confilctTerm int) {
	searchIdx := prevLogIndex-1
	for ;searchIdx>=0; searchIdx--{
		if rf.log[searchIdx].Term == confilctTerm {
			break
		}
	}
	if searchIdx != -1 {
		rf.nextIndex[peer] = rf.log[searchIdx].Index
	}else{
		rf.nextIndex[peer] = confictIndex
	}
}

func (rf *Raft) checkCommit() {
	for {
		rf.mu.Lock()
		if rf.role == Leader {
			commitIdx := rf.getLastLog().Index
			for ;commitIdx>rf.commitIndex;{
				replicate := 1
				for peer := range rf.peers{
					if peer == rf.me{
						continue
					}
					if rf.matchIndex[peer] >= commitIdx {
						replicate++
					}
				}
				if replicate > len(rf.peers)/2 && rf.log[commitIdx-1].Term == rf.currentTerm{
					rf.commitIndex = commitIdx
					break
				}
				commitIdx--
			}	
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50)*time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	for {
		rf.mu.Lock()
		if rf.lastApplied != rf.commitIndex {
			for i:=0; i<=len(rf.log); i++{
				var log Log
				if i==0 {
					log = Log{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
				}else {
					log = rf.log[i-1]
				}
				if log.Index > rf.lastApplied && log.Index <= rf.commitIndex {
					if i==0 {
						msg := ApplyMsg{
							SnapshotValid: true,
							SnapshotIndex: rf.lastIncludedIndex,
							SnapshotTerm: rf.lastIncludedTerm,
							Snapshot: rf.snapShot,
						}
						rf.applyCh <- msg
						rf.lastApplied = rf.lastIncludedIndex
						DPrintf("[server %d] apply a snapshot : %+v", rf.me, msg)
					}else {
						msg := ApplyMsg {
							CommandValid: true,
							Command: log.Command,
							CommandIndex: log.Index,
						}
						rf.applyCh <- msg
						rf.lastApplied = log.Index
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50)*time.Millisecond)
	}
}