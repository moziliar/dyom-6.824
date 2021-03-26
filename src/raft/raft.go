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
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	// All in milliseconds
	electionTimeoutLower = 500
	electionTimeoutUpper = 550
	heartbeatInterval = 100
)

type status int

const (
	follower status = iota
	candidate
	leader
)

func generateElectionTimeout() time.Duration {
	n := rand.Intn(electionTimeoutUpper - electionTimeoutLower)
	return electionTimeoutLower * time.Millisecond + time.Duration(n) * time.Millisecond
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	leader      int
	log         []string

	// Volatile state on all servers
	nodeStatus      status
	commitIndex 	int
	lastApplied     int
	electionTimeout time.Time

	// Volatile state on leader
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.leader == rf.me

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

type AppendEntriesArgs struct {
	Term 			   int
	LeaderID 		   int
	PrevLogIndex 	   int
	PrevLogTermEntries []string
	LeaderCommit       int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}

	rf.currentTerm = args.Term
	rf.leader = args.LeaderID
	rf.votedFor = -1
	rf.electionTimeout = time.Now().Add(generateElectionTimeout())
	rf.nodeStatus = follower

	reply.Term = rf.currentTerm
	reply.Success = true
	//fmt.Printf("%d receive heartbeat from %d as %d in term %d, timeout in %v\n",
	//	rf.me, args.LeaderID, rf.nodeStatus, rf.currentTerm, rf.electionTimeout.Sub(time.Now()).String())

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.mu.Lock()
	//fmt.Printf("%d sends heartbeat to %d as %d in term %d, leader now is %d\n", rf.me, server, rf.nodeStatus, rf.currentTerm, rf.leader)
	//rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 	     int
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
	Term   		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%d received request vote from %d with term %d in term %d, voted for %d\n",
	//	rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

	if rf.currentTerm > args.Term ||
		rf.currentTerm == args.Term && rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	//fmt.Printf("%d sends vote to %d in term %d\n", rf.me, args.CandidateId, args.Term)
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.leader = -1
	rf.electionTimeout = time.Now().Add(generateElectionTimeout())

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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
	//fmt.Println(rf.me, " sending request vote to ", server, "as", rf.nodeStatus)
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
	//fmt.Printf("!!!!!!! node %d is killed now\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.leader = -1
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimeout = time.Now().Add(generateElectionTimeout())
	rf.leader = -1
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.nodeStatus = follower

	go rf.heartbeatDaemon()
	go rf.electionDaemon()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) electionDaemon() {
	for {
		if rf.killed() {
			//fmt.Printf("== in election loop == node %d is killed and sleeps now, leader is %d\n", rf.me, rf.leader)
			return
		}

		rf.mu.Lock()
		electionTO := rf.electionTimeout
		isLeader := rf.leader == rf.me
		//fmt.Printf("node %d checking election status, isLeader is %v\n", rf.me, isLeader)
		if isLeader || time.Now().Before(electionTO) {
			newElectionTimeout := generateElectionTimeout()
			rf.electionTimeout = time.Now().Add(newElectionTimeout)
			rf.mu.Unlock()
			time.Sleep(newElectionTimeout)

			continue
		}

		rf.nodeStatus = candidate
		rf.leader = -1
		rf.votedFor = rf.me
		rf.currentTerm++

		myCurrentTerm := rf.currentTerm
		numOfPeers := len(rf.peers)
		halfVoteFloored := numOfPeers / 2
		rf.mu.Unlock()

		votesAcquired := 1
		nodesReplied := 0
		//fmt.Println("node", rf.me, "become candidate and start requesting votes in term", rf.currentTerm)

		voteChan := make(chan int, numOfPeers-1)
		replyChan := make(chan int, numOfPeers-1)

		for i := range rf.peers {
			if rf.killed() {
				//fmt.Printf("== in election rally loop == node %d is killed and sleeps now, leader is %d\n", rf.me, rf.leader)
				return
			}

			rf.mu.Lock()
			myID := rf.me
			myStatus := rf.nodeStatus
			rf.mu.Unlock()

			if i == myID {
				continue
			} else if myStatus == follower {
				break
			}

			go func(i int, rf *Raft, ch chan<- int) {
				args := RequestVoteArgs{
					Term: myCurrentTerm,
					CandidateId: myID,
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, &args, &reply)

				switch {
				case reply.VoteGranted:
					ch<-1
				case reply.Term >= myCurrentTerm:
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
				default:
					// undefined case?
				}
				replyChan<-1
			}(i, rf, voteChan)
		}

MonitorLoop:
		for {
			select {
			case <-voteChan:
				votesAcquired++
				if votesAcquired <= halfVoteFloored {
					continue
				}

				break MonitorLoop
			case <-replyChan:
				nodesReplied++
				if nodesReplied >= numOfPeers - 1 {
					break MonitorLoop
				}
			}
		}

		//fmt.Println("votes acquired", votesAcquired)

		rf.mu.Lock()
		if votesAcquired <= halfVoteFloored ||
			rf.nodeStatus == follower ||
			rf.currentTerm > myCurrentTerm {
			rf.electionTimeout = time.Now().Add(generateElectionTimeout())
			rf.mu.Unlock()

			continue
		}

		rf.leader = rf.me
		rf.electionTimeout = time.Now().Add(generateElectionTimeout())
		rf.votedFor = -1
		rf.nodeStatus = leader
		//fmt.Printf("%d has become the leader in term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		rf.sendHeartBeat()
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	//fmt.Printf("leader %d term %d\n", rf.leader, rf.currentTerm)
	myCurrentTerm := rf.currentTerm
	myID := rf.me
	rf.mu.Unlock()

	for i := range rf.peers {
		if rf.killed() {
			//fmt.Printf("== in heartbeat == node %d is killed and sleeps now, leader is %d\n", rf.me, rf.leader)
			return
		}

		if i == myID {
			continue
		}

		args := AppendEntriesArgs{
			Term: myCurrentTerm,
			LeaderID: myID,
		}
		//fmt.Println(myID, "sending heartbeat to", i)
		rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	}


}

func (rf *Raft) heartbeatDaemon() {
	for {
		if rf.killed() {
			//fmt.Printf("== in heartbeat loop == node %d is killed and sleeps now, leader is %d\n", rf.me, rf.leader)
			return
		}

		rf.mu.Lock()
		isLeader := rf.leader == rf.me
		//fmt.Printf("node %d isLeader is %v\n", rf.me, isLeader)
		rf.mu.Unlock()

		if isLeader {
			go rf.sendHeartBeat()
		}

		time.Sleep(heartbeatInterval * time.Millisecond)
	}
}
