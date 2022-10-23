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
	// "crypto/aes"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

//
// A Go object implementing a single Raft peer.
//

type Log struct{
	Command string
	Index   int64
	Term   int64
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int64
	logEntries  []Log
	isLeader    bool
	voteFor     map[int64]bool
	refreshTicker  chan int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := int(rf.currentTerm)
	// Your code here (2A).
	isleader := rf.isLeader
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
	CurrentTerm	int64
	LastLog     *Log
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Vote        bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("slave %d recive a vote request %+v, is it voted else? %+v",rf.me, args, rf.voteFor[args.CurrentTerm])


	if voted, ok := rf.voteFor[args.CurrentTerm]; ok && voted{
		reply.Vote = false
		return
	}

	if len(rf.logEntries) == 0 {
		if rf.currentTerm <= args.CurrentTerm {
			rf.currentTerm = args.CurrentTerm
			reply.Vote = true
			rf.voteFor[args.CurrentTerm] = true
			return
		}else{
			reply.Vote = false
			return
		}
	}
	
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	if args.LastLog == nil || args.CurrentTerm < rf.currentTerm || args.LastLog.Term < lastLog.Term{
		reply.Vote = false
		return
	}
	
	if args.LastLog.Term >= lastLog.Term {
		if args.LastLog.Term > lastLog.Term || args.LastLog.Index >= lastLog.Index{
			reply.Vote = true
			rf.voteFor[args.CurrentTerm] = true
			rf.currentTerm = args.CurrentTerm
			return
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// Your code here (2A, 2B).
	// log.Printf("sendRequestVote call args : %+v", args)
	c := make(chan bool)
	go func ()  {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		c<-ok
	}()
	select {
	case ok := <-c:
		// log.Printf("sendRequestVote call res : %+v, reply : %+v", ok, reply)
		return ok
	case <-time.After(time.Duration(15)*time.Millisecond):
		// log.Printf("sendRequestVote call time out server is %d", server)
		return false
	}
}

type AppendEntriesArgs struct{
	LogEntries []Log // appendEntries
	PrevLog    Log // prevLog
	Term       int
}

type AppendEntriesReply struct{
	Recevied bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("slave %d recive AE from term %d",rf.me, args.Term)

	rf.currentTerm = int64(args.Term)
	if rf.isLeader {
		rf.isLeader = false
	}
	if len(rf.refreshTicker) == 0{
		rf.refreshTicker <- 1
	}
}

func (rf *Raft) sendAppendEntries(server int ,args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	c := make(chan bool)

	go func ()  {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		c <- ok
	}()

	select {
	case ok := <-c:
		return ok
	case <-time.After(time.Duration(15)*time.Millisecond):
		return false
	}
}

func (rf *Raft) heartsbeatsTicker() {
	for {
		rf.mu.Lock()
		rf.heartsbeats()
		rf.mu.Unlock()
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

func (rf *Raft) heartsbeats(){
	// log.Printf("%d slave is leader ? %+v",rf.me, rf.isLeader)
	if rf.isLeader {
		// log.Printf("%d slave start heartsbeat", rf.me)
		if len(rf.refreshTicker) == 0{
			rf.refreshTicker<-1
		}
		wg := &sync.WaitGroup{}
		for server := range rf.peers{
			if server == rf.me {
				continue
			}
			wg.Add(1)
			go func (server int)  {
				args := &AppendEntriesArgs{
					Term: int(rf.currentTerm),
				}
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(server, args, reply)
				wg.Done()
			}(server)
		}
		wg.Wait()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed(){

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		source := rand.NewSource(int64(rand.Intn(int(time.Now().Unix()*int64(rf.me+1)))))
		random := rand.New(source)
		
		sleepTime := 150*(1+random.Float64())
		// Output(fmt.Sprintf("slave %d sleep time : %+v", rf.me, sleepTime))
		// log.Printf("slave %d sleep time : %+v",rf.me ,sleepTime)
		time.Sleep(time.Duration(sleepTime)*time.Millisecond)
	
		if len(rf.refreshTicker) > 0 {
			<-rf.refreshTicker
			continue
		}

		rf.mu.Lock()

		if rf.isLeader {
			rf.mu.Unlock()
			continue
		}
		
		rf.currentTerm++
		// log.Printf("slave %d start leader election term : %+v", rf.me, rf.currentTerm)

		rf.voteFor[rf.currentTerm] = true

		wg := &sync.WaitGroup{}
		voted := 1
		voteReply := make(chan bool, len(rf.peers)-1)
		wg.Add(1)
		go func (voteReply chan bool)  {
			recvies := 0
			for res := range voteReply{
				recvies++
				if res {
					voted++
				}
				// log.Printf("slave %d recive :%+v, cnt : %+v", rf.me, res, voted)
				if voted > len(rf.peers)/2{
					rf.isLeader = true
					break
				}
				if recvies == len(rf.peers)-1{
					break
				}
			}
			wg.Done()
		}(voteReply)
		for server, _ := range rf.peers{
			if server == rf.me{
				continue
			} 
			wg.Add(1)
			go func (server int)  {
				// Output(fmt.Sprintf("slave %d request vote term %d from %d", rf.me, rf.currentTerm, server))
				// log.Printf("slave %d request vote term %d from %d", rf.me, rf.currentTerm, server)
				args := &RequestVoteArgs{
					CurrentTerm: rf.currentTerm,
				}
				if len (rf.logEntries) > 0{
					args.LastLog = &rf.logEntries[len(rf.logEntries)-1]
				}
				reply := &RequestVoteReply{}
				// log.Printf("slave %d request vote term %d from %d reply", rf.me, rf.currentTerm, server)
				// log.Printf("end request vote : %+v, currentTerm : %+v, res : %+v, voted : %+v",rf.me, rf.currentTerm, rf.isLeader, voted)
				ok := rf.sendRequestVote(server, args, reply)
				voteReply <- (ok && reply.Vote)
				wg.Done()
				// log.Printf("[request server : %+v] slave %d  currentTerm : %+v, res : %+v",server,rf.me, rf.currentTerm, ok&&reply.Vote)
			}(server)
		}
		wg.Wait()
		close(voteReply)
		rf.heartsbeats()
		// log.Printf("slave %d end leader election res : %+v, currentTerm : %+v",rf.me,rf.isLeader, rf.currentTerm)
		rf.mu.Unlock()
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
	rf.currentTerm = 1
	rf.isLeader = false
	rf.logEntries = make([]Log, 0)
	rf.voteFor = make(map[int64]bool)
	rf.refreshTicker = make(chan int, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartsbeatsTicker()


	return rf
}
