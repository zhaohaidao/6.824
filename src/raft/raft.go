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
	"context"
	"fmt"
)

// import "bytes"
// import "encoding/gob"

const MinimumElectionTimeout = 450
const MaximumElectionTimeout = 2 * MinimumElectionTimeout
const HeartBeatInterval = 100

const (
	Follower = "Follower"
	Candidate = "Candidate"
	Leader = "Leader"
)

type ElectionTimeoutError struct {
	error
	message string
}

func (e *ElectionTimeoutError) Error() string {
	return fmt.Sprintf("%s", e.message)
}

type StateChangeError struct {
	error
	message string
}

func (e *StateChangeError) Error() string {
	return fmt.Sprintf("%s", e.message)
}




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

	commitIndex int
	lastApplied int

	nextIndex     []int
	matchIndex    []int

	actor           string

	changedActor chan string
	resetElectionTimerSignal chan bool
	startElectionSignal chan bool
	newCommitIndexCh chan int
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
	isleader = rf.actor == Leader
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
	Entries      []Log

	LeaderCommit int
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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	for {
		if args.Term < rf.currentTerm {
			reply.Success = false
			break
		} else {
			if rf.currentTerm != args.Term {
				rf.currentTerm = args.Term
			}
			if rf.actor != Follower {
				rf.convert2State(Follower)
			} else {
				rf.resetElectionTimerSignal <- true
			}
		}
		if !((rf.getLastLogIndex() >= args.PrevLogIndex) && (args.PrevLogTerm == rf.logs[args.PrevLogIndex].Term)) {
			reply.Success = false
			break
		}
		if args.Entries != nil {
			if rf.getLastLogIndex() == args.PrevLogIndex {
				rf.logs = append(rf.logs, args.Entries...)
			} else {
				for logIndex := args.PrevLogIndex + 1; logIndex <= args.PrevLogIndex + len(args.Entries); logIndex++ {
					if rf.logs[logIndex].Term != args.Entries[logIndex-args.PrevLogIndex-1].Term {
						rf.logs = rf.logs[0 : logIndex]
						rf.logs = append(rf.logs, args.Entries[logIndex-args.PrevLogIndex-1:]...)
					}

				}
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			rf.newCommitIndexCh <- rf.commitIndex
		}
		reply.Success = true
		break
	}
}

func min(left int, right int) int {
	if left < right {
		return left
	} else {
		return right
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	for {
		if args.Term < rf.currentTerm {
			reply.VoteGranted = false
			break
		} else if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
			if rf.actor != Follower {
				rf.convert2State(Follower)
			}
		}

		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			break
		}

		if args.LastLogTerm > rf.getLastLogTerm() {
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.getLastLogTerm() {
			reply.VoteGranted = args.LastLogIndex >= rf.getLastLogIndex()
		} else {
			reply.VoteGranted = false
		}
		break
	}
	if reply.VoteGranted == true {
		rf.votedFor = args.CandidateId
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) requestVoteReplyHandler(currentTerm int, reply RequestVoteReply, biggerTerm chan int,
	majority chan bool, count *SuccessCount) {
	if reply.Term > currentTerm {
		biggerTerm <- reply.Term
		return
	}
	if reply.VoteGranted {
		count.plusOne()
		if count.get() >= (len(rf.peers)/2 + 1) {
			majority <- true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) needUpdateCommitIndex(entryIndex int) bool {
	if entryIndex <= rf.commitIndex {
		return false
	} else {
		count := 1
		for index := range rf.matchIndex {
			if index != rf.me {
				if rf.matchIndex[index] >= entryIndex && rf.currentTerm == rf.logs[entryIndex].Term {
					count +=1
				}
				if count >= (len(rf.peers)/2 + 1) {
					return true
				}
			}
		}
		return false
	}
}

type Status struct {
	peers       []int
	currentTerm int
	nextIndex   []int
	commitIndex int
	logs        []Log
	lastLogIndex int
	lastLogTerm  int
}


func (rf *Raft) status() Status {
	nextIndexCopy := make([]int, len(rf.nextIndex))
	copy(nextIndexCopy, rf.nextIndex[0:len(rf.nextIndex)])
	return Status{
		peers: make([]int, len(rf.peers)),
		currentTerm: rf.currentTerm,
		nextIndex: nextIndexCopy,
		commitIndex: rf.commitIndex,
		logs: rf.logs,
		lastLogIndex: rf.getLastLogIndex(),
		lastLogTerm: rf.getLastLogTerm(),
	}
}

func (rf *Raft) append(command interface{}) {
	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Command: command})
}

// if lastLogIndex >= nextIndex for a follower, this lastLogIndex can be know only after heartbeat
// upon election, initial heartbeat does not contain entries
func (rf *Raft) getEntries(logs []Log, nextIndex int) []Log {
	if rf.getLastLogIndex() >= nextIndex {
		return logs[nextIndex:]
	} else {
		return nil
	}
}

func (rf *Raft) appendEntriesReplyHandler(reply *AppendEntriesReply, index int, status Status, entries []Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.actor != Leader {
		return
	}
	if reply.Term > status.currentTerm {
		rf.currentTerm = reply.Term
		if rf.actor != Follower {
			rf.convert2State(Follower)
		}
		return
	}
	if !reply.Success {
		rf.nextIndex[index] = status.nextIndex[index] - 1
		return
	} else {
		nextIndex := status.nextIndex[index]
		prevLogIndex := nextIndex - 1
		if rf.getLastLogIndex() >= nextIndex {
			newIndex := prevLogIndex + len(entries)
			rf.nextIndex[index] = newIndex + 1
			rf.matchIndex[index] = newIndex
			if rf.needUpdateCommitIndex(newIndex) {
				DPrintf("peer replicate from %d to %d. commitIndex update from %d to %d",
					rf.me, index, rf.commitIndex, newIndex)
				rf.commitIndex = newIndex
				rf.newCommitIndexCh <- newIndex
			}
		}
	}
}

func (rf *Raft) makeAppendEntriesArgs(status Status) map[int]*AppendEntriesArgs {
	argsMap := make(map[int]*AppendEntriesArgs)
	for index := 0; index < len(status.peers); index++ {
		if index != rf.me {
			nextIndex := status.nextIndex[index]
			prevLogIndex := nextIndex - 1
			entries := rf.getEntries(status.logs, status.nextIndex[index])
			argsMap[index] = &AppendEntriesArgs{Term: status.currentTerm, PrevLogIndex: prevLogIndex,
				PrevLogTerm: status.logs[prevLogIndex].Term, Entries: entries, LeaderCommit: status.commitIndex}
		}
	}
	return argsMap
}

func (rf *Raft) sendAppendEntriesAsync(ctx context.Context, peerIndex int, status Status, args *AppendEntriesArgs,
	waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	rpcOk := make(chan bool)
	reply := &AppendEntriesReply{}
	go func() {
		start := time.Now()
		DPrintf("peer replicate from %d to %d start. prevLogIndex: %d, nextIndex: %d, start: %s",
			rf.me, peerIndex, args.PrevLogIndex, args.PrevLogIndex + 1, start)
		ok := rf.sendAppendEntries(peerIndex, args, reply)
		DPrintf("replicate from %d to %d. isOk: %v, success: %v, Term: %d, currentTerm: %d, start: %s, cost: %s",
			rf.me, peerIndex, ok, reply.Success, reply.Term, status.currentTerm, start, time.Since(start))
		rpcOk <- ok
	}()
	select {
	case ok := <-rpcOk:
		if ok {
			rf.appendEntriesReplyHandler(reply, peerIndex, status, args.Entries)
		}
		DPrintf("replicate from %d to %d finished", rf.me, peerIndex)
	case <- ctx.Done():
		DPrintf("replicate from %d to %d timeout", rf.me, peerIndex)
	}
}

func (rf *Raft) replicate(ctx context.Context, command interface{}, newCommitIndex chan int) {
	DPrintf("peer %d start to replicate", rf.me)
	rf.mu.Lock()
	if rf.actor != Leader {
		return
	}
	if command != nil {
		rf.append(command)
	}
	status := rf.status()
	argsMap := rf.makeAppendEntriesArgs(status)
	rf.mu.Unlock()
	var wg sync.WaitGroup
	done := make(chan bool)
	for index := 0; index < len(status.peers); index++ {
		if index != rf.me {
			wg.Add(1)
			innerCtx, _ := context.WithCancel(ctx)
			go rf.sendAppendEntriesAsync(innerCtx, index, status, argsMap[index], &wg)
		}
	}
	go func() {
		wg.Wait()
		done <- true
	}()
	select {
	case <- ctx.Done():
		DPrintf("replicate timeout for %d", rf.me)
	case <- done:
		DPrintf("replicate finished for peer %d", rf.me)
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
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//// Your code here (2B).
	if rf.actor == Leader {
		rf.append(command)
		index = rf.getLastLogIndex()
		term = rf.getLastLogTerm()
		isLeader = true
	} else {
		isLeader = false
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

func randomElectionTimeout() int {
	return rand.Intn(MaximumElectionTimeout- MinimumElectionTimeout) + MinimumElectionTimeout
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1

}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

type SuccessCount struct {
	count     int
	mu        sync.Mutex
}

func (successCount *SuccessCount) plusOne() {
	successCount.mu.Lock()
	successCount.count += 1
	successCount.mu.Unlock()
}

func (successCount *SuccessCount) get() int {
	successCount.mu.Lock()
	defer successCount.mu.Unlock()
	return successCount.count
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.actor == Leader
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.actor == Candidate
}

func (rf *Raft) isFollower() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.actor == Follower
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
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs= make([]Log, 1)
	rf.logs[0].Term = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for index := 0; index < len(rf.peers); index++ {
		rf.nextIndex[index] = len(rf.logs)
		rf.matchIndex[index] = 0
	}
	rf.resetElectionTimerSignal = make(chan bool)
	rf.changedActor = make(chan string)
	rf.newCommitIndexCh = make(chan int)

	go func() {
		for {
			select {
			case newCommitIndex := <-rf.newCommitIndexCh:
				DPrintf("peer %d will update lastApplied from newCommitIndex %d",
					me, newCommitIndex)
				rf.mu.Lock()
				if newCommitIndex > rf.lastApplied {
					originLastApplied := rf.lastApplied
					for commitIndex := originLastApplied + 1; commitIndex <= newCommitIndex; commitIndex ++ {
						rf.lastApplied = commitIndex
						applyCh <- ApplyMsg{Index: commitIndex, Command: rf.logs[commitIndex].Command}
					}
				}
				rf.mu.Unlock()
			}
		}
	}()

	go rf.stateChangeHandler()
	rf.convert2State(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) stateChangeHandler() {
	for {
		select {
		case newActor := <- rf.changedActor:
			if newActor == Leader {
				go rf.leaderHandler()
			} else if newActor == Candidate {
				go rf.candidateHandler()
			} else {
				go rf.followerHandler()
			}
		}
	}
}

func (rf *Raft) candidateInitialize() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

func (rf *Raft) candidateHandler() {
	for {
		if rf.isCandidate() {
			rf.mu.Lock()
			rf.candidateInitialize()
			term := rf.currentTerm
			rf.mu.Unlock()
			timeout := time.Duration(randomElectionTimeout()) * time.Millisecond
			err := rf.electWithTimeout(timeout)
			if err == nil {
				return
			} else if _, ok := err.(*ElectionTimeoutError); !ok {
				DPrintf("peer %d election failed with term %d reason: %s", rf.me, term, err.Error())
				return
			} else {
				DPrintf("peer %d election timeout with term %d. will start election again", rf.me, term)
			}
		}
	}
}

func (rf *Raft) leaderHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for index := 0; index < len(rf.peers); index++ {
		rf.nextIndex[index] = rf.getLastLogIndex() + 1
		rf.matchIndex[index] = 0
	}
	//rf.append(0)
	go rf.heartbeat()
}

func (rf *Raft) followerHandler() {
	for {
		timeout := time.Duration(randomElectionTimeout()) * time.Millisecond
		select {
		case <-time.After(timeout):
			DPrintf("election timer elapse without heartbeat for: %d, timeout: %s",
				rf.me, timeout)
			rf.mu.Lock()
			rf.convert2State(Candidate)
			rf.mu.Unlock()
			return
		case <-rf.resetElectionTimerSignal:
			DPrintf("election timer is reset for peer: %d", rf.me)
		}
	}
}

func (rf *Raft) elect(ctx context.Context) error {
	rf.mu.Lock()
	status := rf.status()
	votedFor := rf.me
	rf.mu.Unlock()
	majorityVote := make(chan bool)
	biggerTerm := make(chan int)
	grantedCount := &SuccessCount{count:1}
	args := &RequestVoteArgs{Term: status.currentTerm, CandidateId: votedFor, LastLogIndex: status.lastLogIndex,
		LastLogTerm: status.lastLogTerm}
	for index := 0; index < len(status.peers); index++ {
		if index != rf.me {
			innerCtx, _ := context.WithCancel(ctx)
			reply := &RequestVoteReply{}
			go func(index int, innerContext context.Context, voteReply *RequestVoteReply) {
				done := make(chan bool)
				go func() {
					done <- rf.sendRequestVote(index, args, voteReply)
				}()
				select {
				case ok := <- done:
					if ok {
						rf.requestVoteReplyHandler(status.currentTerm, *voteReply, biggerTerm, majorityVote, grantedCount)
					}
				case <- innerContext.Done():
					return
				}
			}(index, innerCtx, reply)
		}
	}
	select {
	case <- majorityVote:
		rf.mu.Lock()
		if rf.actor == Candidate {
			rf.convert2State(Leader)
		}
		rf.mu.Unlock()
		return nil
	case <- ctx.Done():
		return &ElectionTimeoutError{message:fmt.Sprintf("election timeout for peer %d", rf.me)}
	case term := <- biggerTerm:
		rf.mu.Lock()
		if rf.actor != Follower {
			rf.currentTerm = term
			rf.convert2State(Follower)
		}
		rf.mu.Unlock()
		return &StateChangeError{message:fmt.Sprintf("peer %d convert to %s with term %d", rf.me, Follower, term)}
	}
}

func (rf *Raft) convert2State(actor string) {
	DPrintf("peer %d converts to %s with currentTerm %d", rf.me, actor, rf.currentTerm)
	rf.actor = actor
	rf.changedActor <- actor
}

func (rf *Raft) electWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return rf.elect(ctx)
}

func (rf *Raft) heartbeat() {
	timeout := time.Duration(HeartBeatInterval) * time.Millisecond
	for {
		if rf.isLeader() {
			elapse := rf.replicateWithTimeout(timeout, nil, nil)
			if !rf.isLeader() {
				break
			}
			if timeout - elapse > 0 {
				time.Sleep(timeout - elapse)
			}
		} else {
			break
		}
	}
}

func (rf *Raft) replicateWithTimeout(timeout time.Duration, command interface{}, newCommitIndex chan int) time.Duration {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout))
	defer cancel()
	start := time.Now()
	rf.replicate(ctx, command, newCommitIndex)
	return time.Since(start)
}
