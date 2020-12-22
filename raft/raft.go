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
import "labrpc"

// import "bytes"
// import "encoding/gob"


const HeartBeatInterval = 100 * time.Millisecond

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	LastHeartTime int64

	State       int // 0:follower, 1: leader, 1000: candidate
	LeaderId    int
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	CommitIndex int
	LastApplied int

	NextIndex    []int
	MatchIndex   []int
	ApplyCh      chan ApplyMsg
	SuccessNumCh chan int
	ErrVote		int
}

type LogEntry struct{
	Command			int
	Term			int
	Index 			int
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool


	term = rf.CurrentTerm
	if rf.LeaderId == rf.me && rf.State == 1 {
		isleader = true
	}else{
		isleader = false
	}
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int

}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int
	VoteGranted		bool
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Logs			[]LogEntry

	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int				// 当前term
	Success			bool			// true if follower contained entry matching prevLogIndex and prevLogTerm

	MatchIndex		int

}

func (rf * Raft) ToFollower(){
	rf.VotedFor = -1
	rf.State = 0
	rf.ErrVote = 0
}

func (rf * Raft) ToCandidate(){
	rf.LeaderId = -1
	rf.VotedFor = rf.me
	rf.State = 1000
	rf.CurrentTerm ++
}

func (rf * Raft) ToLeader(){
	rf.VotedFor = -1
	rf.LeaderId = rf.me
	rf.State = 1

	for i:= 0; i<len(rf.peers); i++ {
		rf.MatchIndex[i] = 0
		rf.NextIndex[i] = len(rf.Logs)
	}

}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	_, _ = DPrintf("RequestVote process, reply end: %d and before its term: %d, args: %+v", rf.me, rf.CurrentTerm, args)
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.LastHeartTime = time.Now().UnixNano()

	if rf.ErrVote > 1 {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm{
		rf.VotedFor = args.CandidateId
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm

	}else if args.Term == rf.CurrentTerm && rf.VotedFor == -1 {
		rf.VotedFor = args.CandidateId
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
	}else if args.Term == rf.CurrentTerm && rf.VotedFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}else if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}

	_, _ = DPrintf("RequestVote process, reply end: %d and after its term: %d, reply=%+v", rf.me, rf.CurrentTerm, reply)

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf * Raft) leaderElection(){
	//_, _ = DPrintf("Raft %d begin to LeaderElection", rf.me)
	var expireTime = time.Duration(100 + rand.Intn(200)) * time.Millisecond

	for{
		time.Sleep(HeartBeatInterval + expireTime)
		nowTime := time.Now().UnixNano()
		//_, _ = DPrintf("leaderElection id=%d, now: %d, lastHeartBeat: %d, minus: %d, expire + HeartBeat: %d ", rf.me, nowTime, rf.LastHeartTime, nowTime-rf.LastHeartTime, expireTime+HeartBeatInterval)

		if (nowTime - rf.LastHeartTime) > int64(expireTime) {
			if rf.State != 1 {
				rf.ToCandidate()
				rf.LastHeartTime = time.Now().UnixNano()
				_, _ = DPrintf("%d begin to send for Vote on term %d", rf.me, rf.CurrentTerm)
				rf.evokeVote()				// 选完才继续，否则阻塞这里，不然会发出多次领导人选举
			}
		}
	}
}

func (rf * Raft) evokeVote(){
	rf.mu.Lock()
	lastLogEntry := rf.GetLastLogEntry()
	args := RequestVoteArgs{
		Term: 			rf.CurrentTerm,
		CandidateId: 	rf.me,
		LastLogIndex:	lastLogEntry.Index,
		LastLogTerm: 	lastLogEntry.Term,
	}
	rf.mu.Unlock()
	numVote := 1
	// 向每一个结点发送投票请求
	for i := 0; i<len(rf.peers); i++{
		if i == rf.me{					// 跳过自己，因为已经投了自己
			continue
		}

		go func(peerId int) {
			reply := RequestVoteReply{}
			_, _ = DPrintf("%d send vote request to %d on term %d", rf.me, peerId, rf.CurrentTerm)
			ok := rf.sendRequestVote(peerId, args, &reply)

			if !ok {					// 没有正确发送成功
				rf.ErrVote += 1
				_, _ = DPrintf("Error rf.sendRequestVote, from: %v, to: %v, args: %+v", rf.me, peerId, args)
				return
			}
			_, _ = DPrintf("rf.sendRequestVote gotton reply, selfID: %v, peerID: %v, reply: %+v", rf.me, peerId, reply)

			rf.mu.Lock()

			if rf.ErrVote > 1 || reply.Term > rf.CurrentTerm {
				rf.ToFollower()
				rf.CurrentTerm = reply.Term
				rf.LastHeartTime = time.Now().UnixNano()
				rf.mu.Unlock()
				return
			}

			// 投票成功
			if reply.VoteGranted{
				numVote ++
				if numVote > len(rf.peers)/2 && rf.State == 1000 {
					rf.ToLeader()
					rf.LastHeartTime = time.Now().UnixNano()
					_, _ = DPrintf("%d become the leader on term: %d", rf.me, rf.CurrentTerm)
					go rf.LoopHeartBeat()
				}
			}
			rf.mu.Unlock()
		}(i)

	}
}

func (rf * Raft) GetLastLogEntry() LogEntry{
	return rf.Logs[len(rf.Logs)-1]
}

func (rf *Raft) sendAppendEntriesToOnePeer(peerId int) {
	// 这里加锁会产生死锁，我也没找到引发的原因……所以就不加锁了
	args := AppendEntriesArgs {
		Term: 				rf.CurrentTerm,
		LeaderId: 			rf.me,
		PrevLogIndex: 		rf.NextIndex[peerId] - 1,
		PrevLogTerm: 		rf.Logs[rf.NextIndex[peerId]-1].Term,
		LeaderCommit: 		rf.CommitIndex,
	}
	if args.PrevLogIndex < len(rf.Logs) {
		args.Logs = rf.Logs[rf.NextIndex[peerId]:]
	}

	_, _ = DPrintf("sendAppendEntriesToOnePeer me=%d, peerId: %d, args: %+v", rf.me, peerId, args)
	go func(peerId int, args AppendEntriesArgs) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntry(peerId, args, &reply)
		if !ok {
			_, _ = DPrintf("%d send a append PRC to %d failed", rf.me, peerId)
			return
		}
		_, _ = DPrintf("receive sendAppendEntriesToOnePeer me=%d, peerId: %d, args: %+v", rf.me, peerId, args)
		rf.handleAppendEntriesReply(peerId, reply)

	}(peerId, args)

}

func (rf *Raft) handleAppendEntriesReply(peerId int, reply AppendEntriesReply) {
	_, _ = DPrintf("handleAppendEntriesReply self:%d, peerId: %d, reply: %+v", rf.me, peerId, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != 1 {
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.ToFollower()
		rf.CurrentTerm = reply.Term
		rf.LastHeartTime = time.Now().UnixNano()
		return
	}

	if reply.Success {
		_, _ = DPrintf("handleAppendEntriesReply reply leader: %d, peerid: %d before nextIndex: %d, matchindex: %d", rf.LeaderId, peerId, rf.NextIndex[peerId], rf.MatchIndex[peerId])
		rf.NextIndex[peerId] = reply.MatchIndex + 1
		rf.MatchIndex[peerId] = reply.MatchIndex

		count := 1
		for i:=0; i<len(rf.peers); i++{
			if i != rf.me && rf.MatchIndex[i] >= reply.MatchIndex {
				count ++
			}
		}
		_, _ = DPrintf("handleAppendEntriesReply reply leader: %d, peerid: %d after nextIndex: %d, matchindex: %d, count=%d", rf.LeaderId, peerId, rf.NextIndex[peerId], rf.MatchIndex[peerId], count)

		if count > len(rf.peers)/2 {
			_, _ = DPrintf("count beyond half, count=%d, ready to commit, rfid=%d,  MatchIndex=%d", count, rf.me, reply.MatchIndex)
			if rf.CommitIndex < reply.MatchIndex && rf.Logs[reply.MatchIndex].Term == rf.CurrentTerm {
				rf.CommitIndex = reply.MatchIndex
				go rf.Commit()
			}
		}
	} else {
		rf.NextIndex[peerId] = reply.MatchIndex + 1
		rf.sendAppendEntriesToOnePeer(peerId)
	}

}

func(rf *Raft)Commit(){
	// _, _ = DPrintf("Commit, rf=%+v", rf)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	i := rf.LastApplied + 1
	for i<= rf.CommitIndex {
		_, _ = DPrintf("Commit, me=%d, CommitIndex=%d, command=%d", rf.me, i, rf.Logs[i].Command)
		args := ApplyMsg{
			Index: 		i,
			Command: 	rf.Logs[i].Command,
		}
		rf.ApplyCh <- args
		i++
	}
	rf.LastApplied = rf.CommitIndex
}

func(rf * Raft) LoopHeartBeat(){
	// _, _ = DPrintf("%d start LoopHeartBeat on term %d", rf.me, rf.CurrentTerm)
	for {
		if rf.State !=1 {
			_, _ = DPrintf("%d stop LoopHeartBeat ", rf.me)
			return
		}

		rf.LastHeartTime = time.Now().UnixNano()

		for i:= 0; i<len(rf.peers); i++ {
			if i != rf.me {
				_, _ = DPrintf("%d start LoopHeartBeat to %d on term %d, rf=%+v", rf.me, i, rf.CurrentTerm, rf)
				go rf.sendAppendEntriesToOnePeer(i)
			}
		}
		time.Sleep(HeartBeatInterval)
	}
}



func (rf * Raft)sendAppendEntry(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf * Raft) AppendEntries(args AppendEntriesArgs, reply * AppendEntriesReply){
	_, _ = DPrintf("AppendEntries before lock to %d arg=%+v, reply=%+v", rf.me, args, reply)
	rf.mu.Lock()
	_, _ = DPrintf("AppendEntries after lock to %d arg=%+v, reply=%+v", rf.me, args, reply)
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm  && args.LeaderCommit <= rf.CommitIndex{			// 添加日志时如果发送方term小于接收方term则拒绝，然后返回一个高的term
		_, _ = DPrintf("%d get append entries from %d, but the term less than %d", rf.me, args.LeaderId, rf.me)
		reply.Term, reply.Success = rf.CurrentTerm, false
		return
	}

	rf.LastHeartTime = time.Now().UnixNano()
	rf.CurrentTerm = args.Term
	rf.LeaderId = args.LeaderId
	rf.ToFollower()
	reply.Term = rf.CurrentTerm

	//_, _ = DPrintf("AppendEntries id=%d, leaderid=%d, len(rf.Logs)=%d, args=%+v, reply=%+v, rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm=%v", rf.me, rf.LeaderId, len(rf.Logs), args, reply, rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm)
	if args.PrevLogIndex >= 0 && (len(rf.Logs)-1 < args.PrevLogIndex || rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm){
		index := len(rf.Logs) - 1
		if index > args.PrevLogIndex {
			index = args.PrevLogIndex
		}

		for index > 0 {   //  index>0
			if args.PrevLogTerm == rf.Logs[index].Term {
				break
			}
			index --
		}
		reply.MatchIndex = index
		reply.Success = false
	} else if args.Logs != nil {
		rf.Logs = rf.Logs[:args.PrevLogIndex+1]
		rf.Logs = append(rf.Logs, args.Logs...)
		if args.LeaderCommit > rf.CommitIndex && len(rf.Logs)-1 >= args.LeaderCommit {
			rf.CommitIndex = args.LeaderCommit
			go rf.Commit()
		}
		reply.MatchIndex = len(rf.Logs) - 1
		reply.Success = true
	}else {
		if args.LeaderCommit > rf.CommitIndex && len(rf.Logs)-1 >= args.LeaderCommit{
			rf.CommitIndex = args.LeaderCommit
			go rf.Commit()
		}
		reply.MatchIndex = args.LeaderCommit
		reply.Success = true
	}
}


func (rf * Raft) startApplyLogs(){
	for rf.LastApplied < rf.CommitIndex {
		rf.LastApplied ++
		msg := ApplyMsg{}
		msg.Index = rf.LastApplied
		msg.Command = rf.Logs[rf.LastApplied-1].Command
		rf.ApplyCh <- msg
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.CurrentTerm
	isLeader := rf.me == rf.LeaderId

	if isLeader && rf.State == 1{
		_, _ = DPrintf("Start Leader %d got a new Start command %v on term %d", rf.me, command, rf.CurrentTerm)
		rf.Logs = append(rf.Logs, LogEntry{
			Command: command.(int),
			Term:    rf.CurrentTerm,
			Index:   len(rf.Logs),
		})
		index = len(rf.Logs) - 1
		term = rf.CurrentTerm
		return index, term, isLeader
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
	rf.State = 0
	rf.VotedFor = -1
	rf.LeaderId = -1
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

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.State	= 0						// 0 follower 1:leader 1000: candidate
	rf.LastHeartTime = time.Now().UnixNano()
	rf.Logs = make([]LogEntry, 0)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.LeaderId = -1
	rf.ApplyCh = applyCh
	rf.ErrVote = 0
	// 先插入一个空Log，避免首尾判断
	rf.Logs = append(rf.Logs, LogEntry{Command:0, Index:0, Term: rf.CurrentTerm})

	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	go rf.leaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

