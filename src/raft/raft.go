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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader

	heartbeatInterval = time.Duration(70) * time.Millisecond
	electionTimeout   = time.Duration(250) * time.Millisecond
	voteTimeout       = electionTimeout

	maxRPCLogEntries = 1000
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // channel on which the tester or service expects Raft to send ApplyMsg messages

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTime time.Time // time of next election
	state        State     // server state (follower, candidate or leader)

	// Persistent state on all servers:
	currentTerm       int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor          int        // candidateId that received vote in current term (or -1 if none)
	log               []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	lastIncludedIndex int        // the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int        // term of lastIncludedIndex

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// LogEntry log entry structure.
type LogEntry struct {
	Index   int         // step by step
	Term    int         // term of the entry
	Command interface{} // command for state machine
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		Debug(dError, "S%d read persist ERROR", rf.me)
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	Debug(dLog, "S%d read persist T%d V%d L%d LIT%d LII%d",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.lastIncludedTerm, rf.lastIncludedIndex)
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index > rf.commitIndex || index <= rf.lastIncludedIndex {
			Debug(dLog, "S%d Snapshot Reject I%d CI%d LII%d", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
			return
		}

		l := index - rf.lastIncludedIndex
		rf.lastIncludedIndex = rf.log[l-1].Index
		rf.lastIncludedTerm = rf.log[l-1].Term

		newLog := make([]LogEntry, len(rf.log)-l)
		copy(newLog, rf.log[l:])
		rf.log = newLog
		rf.persist()
		rf.persister.Save(rf.persister.ReadRaftState(), snapshot)

		Debug(dLog, "S%d Snapshot T%d I%d L%d LIT%d LII%d", rf.me, rf.currentTerm, index, len(rf.log), rf.lastIncludedTerm, rf.lastIncludedIndex)
	}()
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Snapshot          []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.Snapshot == nil || len(args.Snapshot) < 1 {
		Debug(dError, "S%d install snapshot ERROR len%d", rf.me, len(args.Snapshot))
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	rf.electionTime = time.Now().Add(electionTimeout)

	if args.Term < rf.currentTerm {
		Debug(dLog, "S%d rejected S%d install snapshot, (T%d < T%d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	l := rf.lastIncludedIndex + len(rf.log)
	if l-args.LastIncludedIndex > 0 &&
		l-args.LastIncludedIndex-1 < len(rf.log) &&
		args.LastIncludedTerm == rf.log[l-args.LastIncludedIndex-1].Term {
		Debug(dLog, "S%d accept S%d repeat snapshot", rf.me, args.LeaderId)
		return
	}

	rf.log = nil
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persister.Save(rf.persister.ReadRaftState(), args.Snapshot)
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

// sendInstallSnapshot send a InstallSnapshot RPC to a server.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// handleAppendEntriesResponse handles InstallSnapshot response.
func (rf *Raft) handleInstallSnapshotResponse(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		Debug(dLeader, "S%d convert Follower, S%d (T%d > T%d)", rf.me, server, reply.Term, rf.currentTerm)
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.persist()
	} else {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		Debug(dLeader, "S%d increase nextIndex[%d] to %d", rf.me, server, rf.nextIndex[server])
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	LastLogIndex  int  // last log index of follower
	ConflictTerm  int  // term of the conflicting entry
	ConflictIndex int  // index of first entry with ConflictTerm
}

// AppendEntries Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTime = time.Now().Add(electionTimeout)

	if args.Term < rf.currentTerm {
		Debug(dLog, "S%d rejected S%d append entries, (T%d < T%d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		Debug(dLog, "S%d convert Follower, S%d (T%d > T%d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.persist()
	}

	l := rf.lastIncludedIndex + len(rf.log)
	if args.PrevLogIndex > 0 {
		if l < args.PrevLogIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.LastLogIndex = l
			return
		}

		pli, plt := args.PrevLogIndex-rf.lastIncludedIndex, 0
		if pli < 0 {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.LastLogIndex = l
			return
		} else if pli == 0 {
			plt = rf.lastIncludedTerm
		} else {
			plt = rf.log[pli-1].Term
		}

		if plt != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.LastLogIndex = l
			if pli == 0 {
				reply.ConflictTerm = rf.lastIncludedTerm
				reply.ConflictIndex = rf.lastIncludedIndex
			} else {
				reply.ConflictTerm = rf.log[pli-1].Term
				reply.ConflictIndex = args.PrevLogIndex
			}
			for i := pli - 1; i >= 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					break
				}
				reply.ConflictIndex = i + 1
			}
			Debug(dLog, "S%d rejected S%d, %d < %d || T ERR", rf.me, args.LeaderId, l, args.PrevLogIndex)
			return
		}
	}

	if len(args.Entries) > 0 {
		entry := args.Entries[0]
		idx := entry.Index - rf.lastIncludedIndex
		if len(rf.log) >= idx {
			if l >= entry.Index+len(args.Entries)-1 && args.Entries[len(args.Entries)-1].Term == rf.log[idx+len(args.Entries)-2].Term {
				reply.Term = rf.currentTerm
				reply.Success = true
				reply.LastLogIndex = l
				Debug(dLog, "S%d accept S%d repeated entries %d", rf.me, args.LeaderId, len(args.Entries))
				return
			}
			rf.log = rf.log[:idx-1]
			Debug(dLog, "S%d truncate log %d", rf.me, idx)
		}

		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		Debug(dLog, "S%d append entries %d L%d", rf.me, len(args.Entries), len(rf.log))
	}

	if args.LeaderCommit > rf.commitIndex {
		var lastLog LogEntry
		if len(rf.log) == 0 {
			lastLog = LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
		} else {
			lastLog = rf.log[len(rf.log)-1]
		}

		// maybe this is right
		//if rf.lastApplied == 0 && len(args.Entries) > 0 && len(rf.log) > 0 {
		//	rf.lastApplied = args.Entries[0].Index - rf.log[0].Index
		//} else {
		// test uses snapshot as lastApplied, so this is not right (maybe)
		if rf.commitIndex == 0 && rf.lastApplied == 0 {
			rf.lastApplied = rf.lastIncludedIndex
		}
		rf.lastApplied = max(rf.lastApplied-rf.lastIncludedIndex, 0)
		//}
		rf.commitIndex = min(args.LeaderCommit, lastLog.Index)
		Debug(dLog, "S%d CI%d AI%d", rf.me, rf.commitIndex, rf.lastApplied)

		leastEntries := rf.log[rf.lastApplied : rf.commitIndex-rf.lastIncludedIndex]
		for _, entry := range leastEntries {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg
			rf.lastApplied = entry.Index
			Debug(dLog, "S%d applyCh <- {%d %v}", rf.me, entry.Index, entry.Command)
			// log.Printf("F%d applyCh <- {%d %v} L%d", rf.me, entry.Index, entry.Command, len(rf.log))
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// sendAppendEntries example code to send a AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// handleAppendEntriesResponse handles AppendEntries response.
func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		Debug(dLeader, "S%d convert Follower, S%d (T%d > T%d)", rf.me, server, reply.Term, rf.currentTerm)
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.persist()
	} else {
		if reply.Success {
			rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
			rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])

			commitCount := 1
			for i, matchIndex := range rf.matchIndex {
				if i != rf.me && matchIndex >= rf.matchIndex[server] {
					commitCount++
				}
			}
			if commitCount > len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[server] {
				rf.commitIndex = rf.matchIndex[server]
				Debug(dLeader, "S%d commit index %d", rf.me, rf.commitIndex)

				leastEntries := rf.log[max(rf.lastApplied-rf.lastIncludedIndex, 0) : rf.commitIndex-rf.lastIncludedIndex]
				for _, entry := range leastEntries {
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandTerm:  entry.Term,
						CommandIndex: entry.Index,
					}
					rf.applyCh <- applyMsg
					rf.lastApplied = entry.Index
					Debug(dLeader, "S%d applyCh <- {%d %v}", rf.me, applyMsg.CommandIndex, applyMsg.Command)
					// log.Printf("L%d applyCh <- {%d %v}", rf.me, applyMsg.CommandIndex, applyMsg.Command)
				}
			}
		} else {
			if args.PrevLogIndex > rf.nextIndex[server] {
				Debug(dLeader, "S%d keep nextIndex[%d] to %d", rf.me, server, rf.nextIndex[server])
				return
			}
			if reply.ConflictTerm != 0 {
				rf.nextIndex[server] = max(min(reply.ConflictIndex, min(args.PrevLogIndex, reply.LastLogIndex+1)), rf.matchIndex[server]+1)
				Debug(dLeader, "S%d conflict, decrement nextIndex[%d] to %d", rf.me, server, rf.nextIndex[server])
				return
			}

			rf.nextIndex[server] = max(min(args.PrevLogIndex, reply.LastLogIndex+1), rf.matchIndex[server]+1)
			Debug(dLeader, "S%d decrement nextIndex[%d] to %d", rf.me, server, rf.nextIndex[server])
		}
	}
}

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTime = time.Now().Add(electionTimeout)

	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d rejected vote, S%d (T%d < T%d)", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		Debug(dVote, "S%d convert Follower, S%d (T%d > T%d)", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	var lastLog LogEntry
	if len(rf.log) == 0 {
		lastLog = LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
	} else {
		lastLog = rf.log[len(rf.log)-1]
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)) {
		rf.votedFor = args.CandidateId
		rf.persist()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		Debug(dVote, "S%d voted, S%d T%d", rf.me, args.CandidateId, rf.currentTerm)
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	Debug(dVote, "S%d rejected vote, S%d T%d", rf.me, args.CandidateId, rf.currentTerm)
}

// sendRequestVote example code to send a RequestVote RPC to a server.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// startElection starts a new election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.electionTime = time.Now().Add(electionTimeout)
	rf.persist()
	Debug(dTerm, "S%d become Candidate, start election (T%d)", rf.me, rf.currentTerm)

	voteCh := make(chan RequestVoteReply, len(rf.peers)-1)
	for server := range rf.peers {
		if server != rf.me {
			var lastLog LogEntry
			if len(rf.log) == 0 {
				lastLog = LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
			} else {
				lastLog = rf.log[len(rf.log)-1]
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLog.Index,
				LastLogTerm:  lastLog.Term,
			}
			go func(server int, args *RequestVoteArgs) {
				Debug(dVote, "S%d -> S%d Asking for vote", rf.me, server)
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					voteCh <- *reply
				}
			}(server, args)
		}
	}
	rf.mu.Unlock()

	voteTimeoutTimer := time.NewTimer(voteTimeout)
	defer voteTimeoutTimer.Stop()

	for commits := 1; commits < len(rf.peers)/2+1; {
		select {
		case voteReply := <-voteCh:
			rf.mu.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				return
			}

			if voteReply.Term > rf.currentTerm {
				Debug(dVote, "S%d convert Follower, (T%d > T%d)", rf.me, voteReply.Term, rf.currentTerm)
				rf.state = Follower
				rf.currentTerm = voteReply.Term
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if voteReply.VoteGranted {
				commits++
			}
		case <-voteTimeoutTimer.C:
			Debug(dVote, "S%d vote timeout, restart election", rf.me)
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			rf.mu.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				Debug(dVote, "S%d restart election fail", rf.me)
				return
			}
			rf.mu.Unlock()
			rf.startElection()
			return
		}
	}

	rf.becomeLeader()
}

// becomeLeader becomes a leader.
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	lastLogIndex := 0
	if rf.lastIncludedIndex > 0 || len(rf.log) > 0 {
		lastLogIndex = rf.lastIncludedIndex + len(rf.log)
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	Debug(dLeader, "S%d become Leader at T%d L%d", rf.me, rf.currentTerm, len(rf.log))
	// log.Printf("S%d become Leader at T%d L%d", rf.me, rf.currentTerm, len(rf.log))

	go rf.sendHeartbeats()
}

// sendHeartbeats sends heartbeats to all peers.
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.electionTime = time.Now().Add(electionTimeout)

		for server := range rf.peers {
			if server != rf.me {
				var prevLog LogEntry
				var entries []LogEntry
				if rf.nextIndex[server] == 1 {
					entries = rf.log[:min(len(rf.log), maxRPCLogEntries)]
				}
				if rf.nextIndex[server] > 1 && rf.lastIncludedIndex+len(rf.log) >= rf.nextIndex[server]-1 {
					if rf.nextIndex[server] <= rf.lastIncludedIndex {
						args := &InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.lastIncludedIndex,
							LastIncludedTerm:  rf.lastIncludedTerm,
							Snapshot:          rf.persister.ReadSnapshot(),
						}

						go func(server int, args *InstallSnapshotArgs) {
							Debug(dLeader, "S%d -> S%d installSnapshot, LIT%d LII%d",
								rf.me, server, args.LastIncludedTerm, args.LastIncludedIndex)
							reply := &InstallSnapshotReply{}
							if rf.sendInstallSnapshot(server, args, reply) {
								rf.handleInstallSnapshotResponse(server, args, reply)
							}
						}(server, args)

						continue
					} else if rf.nextIndex[server] == (rf.lastIncludedIndex + 1) {
						prevLog = LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
						entries = rf.log[:min(len(rf.log), maxRPCLogEntries)]
					} else {
						prevLog = rf.log[rf.nextIndex[server]-2-rf.lastIncludedIndex]
						entries = rf.log[rf.nextIndex[server]-1-rf.lastIncludedIndex : min(len(rf.log), rf.nextIndex[server]-1-rf.lastIncludedIndex+maxRPCLogEntries)]
					}
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLog.Index,
					PrevLogTerm:  prevLog.Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				go func(server int, args *AppendEntriesArgs) {
					Debug(dLeader, "S%d -> S%d heartbeat, PL(I%d, T%d), %d ",
						rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
					// log.Printf("S%d -> S%d heartbeat, PL(I%d, T%d), %d ", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(server, args, reply) {
						rf.handleAppendEntriesResponse(server, args, reply)
					}
				}(server, args)
			}
		}
		rf.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader

	if !isLeader {
		return
	}

	index = rf.lastIncludedIndex + len(rf.log) + 1
	newLogEntry := LogEntry{Index: index, Term: term, Command: command}
	rf.log = append(rf.log, newLogEntry)
	rf.persist()

	for server := range rf.peers {
		if server != rf.me {
			prevLog := LogEntry{
				Index: rf.lastIncludedIndex,
				Term:  rf.lastIncludedTerm,
			}
			entries := []LogEntry{newLogEntry}
			if len(rf.log) > 1 {
				prevLog = rf.log[len(rf.log)-2]
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			go func(server int, args *AppendEntriesArgs) {
				Debug(dLeader, "S%d -> S%d args %v", rf.me, server, args)
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(server, args, reply) {
					rf.handleAppendEntriesResponse(server, args, reply)
				}
			}(server, args)
		}
	}

	Debug(dLeader, "S%d Start, T%d I%d CMD%v", rf.me, term, index, command)
	return
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker is a ticker goroutine.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		isFollower := rf.state == Follower
		isElectionTimeout := time.Now().After(rf.electionTime)
		//Debug(dLog, "S%d electionTime = %v now = %v", rf.me, rf.electionTime.Format("05.000"), time.Now().Format("05.000"))
		rf.mu.Unlock()

		if isFollower && isElectionTimeout {
			Debug(dVote, "S%d election timeout, start election", rf.me)
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.electionTime = time.Now().Add(electionTimeout)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = nil
	rf.commitIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
