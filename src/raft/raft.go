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
// rf.GetState() (term, lsLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

const MinimumElectionTimeout = 350
const MaxmumElectionTimeout = 2 * MinimumElectionTimeout
const HeartBeatInterval = 100

const (
	Follower = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs         []Log

	committedIndex int
	lastApplied   int

	nextIndex     []int
	matchIndex    []int

	actor           int
	latestTimerAt   time.Time
	electionTimeout int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.actor == Leader
	rf.mu.Unlock()
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}


// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	for {
		if args.Term < rf.currentTerm {
			reply.Success = false
			break
		} else if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			if rf.actor != Follower {
				oldActor := rf.actor
				rf.actor = Follower
				DPrintf("peer %d convert from %d to follower in AppendEntries, currentTerm: %d", rf.me, oldActor, rf.currentTerm)
			}
		}
		rf.resetElectionTimer()
		reply.Success = args.PrevLogIndex == rf.getLastLogIndex() && args.PrevLogTerm == rf.getLastLogTerm()
		break
	}
	rf.mu.Unlock()

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	for {
		if args.Term < rf.currentTerm {
			reply.VoteGranted = false
			break
		} else if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.actor = Follower
			rf.votedFor = -1
			DPrintf("peer %d convert to follower in RequestVote, currentTerm: %d", rf.me, rf.currentTerm)
		}

		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			break
		}
		reply.VoteGranted = true
		//lastLogIndex := len(rf.logs) - 1
		//lastLogTerm := rf.logs[len(rf.logs) - 1].Term
		//
		//if args.LastLogTerm > lastLogTerm {
		//	reply.VoteGranted = true
		//} else if args.LastLogTerm == lastLogTerm {
		//	reply.VoteGranted = args.LastLogIndex >= lastLogIndex
		//} else {
		//	reply.VoteGranted = false
		//}
		break
	}
	if reply.VoteGranted == true {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
	}
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimer()  {
	rf.electionTimeout = randomElectionTimeout()
	rf.latestTimerAt = time.Now()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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

func (rf *Raft) electionTimeoutElapse() bool {
	return time.Now().Sub(rf.latestTimerAt) > time.Duration(time.Duration(rf.electionTimeout) * time.Millisecond)
}

func randomElectionTimeout() int {
	return rand.Intn(MaxmumElectionTimeout - MinimumElectionTimeout) + MinimumElectionTimeout
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1

}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

type VoteGrantedCount struct {
	count     int
	mu        sync.Mutex
}

// TODO: 还是要搞清楚go的指针和值的区别
func (voteGrantedCount *VoteGrantedCount) plusOne() {
	voteGrantedCount.mu.Lock()
	voteGrantedCount.count += 1
	voteGrantedCount.mu.Unlock()
}

func (voteGrantedCount *VoteGrantedCount) get() int {
	voteGrantedCount.mu.Lock()
	defer voteGrantedCount.mu.Unlock()
	return voteGrantedCount.count
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initialy holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs= make([]Log, 10000)
	rf.logs[0].Term = 0

	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.actor = Follower
	rf.resetElectionTimer()
	rf.mu.Unlock()

	// election
	go func() {
		for {
			// not a ideal implementation
			// variable timer maybe better. but I have to solve one problem as followed
			// we have to lock the race condition to make sure not read stale data, this is the bad part
			// in Critical Section, if meet election condition, we start to elect
			// but if not, we have to unlock. this if-else logic makes code dirty
			rf.mu.Lock()
			if rf.actor != Leader && rf.electionTimeoutElapse() {
				if rf.actor == Follower {
					rf.actor = Candidate
					DPrintf("%d convert to candidate. electionTimeout: %d, currentTerm: %d",
						rf.me, rf.electionTimeout, rf.currentTerm)
				} else {
					DPrintf("%d re-election.", rf.me)
				}
				rf.currentTerm += 1
				rf.votedFor = rf.me
				currentTerm := rf.currentTerm
				votedFor := rf.votedFor
				rf.resetElectionTimer()
				lastLogIndex := rf.getLastLogIndex()
				lastLogTerm := rf.getLastLogTerm()
				me := rf.me
				timeout := time.After(time.Duration(rf.electionTimeout) * time.Millisecond)
				rf.mu.Unlock()
				majorityVote := make(chan bool)
				biggerTerm := make(chan int)
				grantedCount := &VoteGrantedCount{count:1}
				for index := 0; index < len(rf.peers); index++ {
					if index != me {
						go func(index int, lastLogIndex int, lastLogTerm int, count *VoteGrantedCount) {
							args := &RequestVoteArgs{Term: currentTerm, CandidateId: votedFor, LastLogIndex: rf.getLastLogIndex(), LastLogTerm: rf.getLastLogTerm()}
							reply := &RequestVoteReply{}
							ok := false
							for !ok {
								ok = rf.sendRequestVote(index, args, reply)
								if ok {
									if reply.VoteGranted {
										count.plusOne()
										if count.get() >= (len(rf.peers)/2 + 1) {
											majorityVote <- true
										}
									}
									if reply.Term > currentTerm {
										biggerTerm <- reply.Term
									}
								} else {
									DPrintf("requestNote from %d to %d failed", me, index)
								}
							}

						}(index, lastLogIndex, lastLogTerm, grantedCount)

					}
				}
				select {
				case <- majorityVote:
					rf.mu.Lock()
					if rf.actor == Candidate {
						rf.actor = Leader
						DPrintf("peer %d become leader, currentTerm: %d", rf.me, rf.currentTerm)
					}
					rf.mu.Unlock()
				case term := <- biggerTerm:
					rf.mu.Lock()
					rf.currentTerm = term
					rf.actor = Follower
					DPrintf("peer %d convert to follower, currentTerm: %d", rf.me, rf.currentTerm)
					rf.mu.Unlock()
				case <- timeout:
					rf.mu.Lock()
					term := rf.currentTerm
					rf.mu.Unlock()
					DPrintf("election timeout for %d, currentTerm: %d, will start to election again\n", rf.me, term)
				}

			} else {
				rf.mu.Unlock()
			}
		}

	}()

	// heartbeat
	go func() {
		for {
			rf.mu.Lock()
			if rf.actor == Leader {
				for index := 0; index < len(rf.peers); index++ {
					if index != me {
						args := &AppendEntriesArgs{rf.currentTerm, me, rf.getLastLogIndex(), rf.getLastLogTerm()}
						reply := &AppendEntriesReply{}
						rf.mu.Unlock()
						ok := rf.sendAppendEntries(index, args, reply)
						rf.mu.Lock()
						if rf.actor != Leader {
							break
						}
						if ok {
							if reply.Term > rf.currentTerm {
								DPrintf("peer %d step down, oldTerm: %d, newTerm: %d", rf.me, rf.currentTerm, reply.Term)
								rf.currentTerm = reply.Term
								rf.actor = Follower
								break
							}
						} else {
							DPrintf("heartbeat from %d to %d failed, currentTerm: %d", rf.me, index, rf.currentTerm)
						}
					}
					rf.mu.Unlock()
					time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
					rf.mu.Lock()
				}
			}
			rf.mu.Unlock()
		}
	}()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
