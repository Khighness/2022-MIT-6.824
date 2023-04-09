package raft

import (
	"6.824/log"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	_ "6.824/log"

	"go.uber.org/zap"
)

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

// Zero is an initial value.
const Zero int = 0

// None is a placeholder node ID used when there is no leader.
const None int = -1

// Progress structure.
// It represents a follower's progress in the view of the leader.
type Progress struct {
	Match, Next int
}

// ApplyMsg structure.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft structure.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	id        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role    NodeRole // Node role
	tick    ticker   // Ticker
	term    int      // Current term
	vote    int      // Voted for
	lead    int      // Leader id
	raftLog *RaftLog // Raft Log

	ballotBox map[int]bool      // Ballot box
	progress  map[int]*Progress // Follower's log replication progress

	applyCh       chan ApplyMsg // Used to send command to state machine
	notifyApplyCh chan struct{} // Used to notify leader apply command

	logger *zap.SugaredLogger
}

// String uses for easy logging.
// It looks like:
//	[role name - peer id : current term]
func (rf *Raft) String() string {
	return fmt.Sprintf("[%s-%v:%d]", rf.role.ShortString(), rf.id, rf.term)
}

// GetState returns the peer's current state and if current peer is leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.isLeader()
}

// isLeader checks if the peer's role is Leader.
func (rf *Raft) isLeader() bool {
	return rf.role == Leader
}

// isLeader checks if the peer's role is Candidate.
func (rf *Raft) isCandidate() bool {
	return rf.role == Candidate
}

// isLeader checks if the peer's role is Follower.
func (rf *Raft) isFollower() bool {
	return rf.role == Follower
}

// persist saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	var err error
	if err = encoder.Encode(rf.term); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.vote); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.raftLog.entries); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.raftLog.committed); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.raftLog.applied); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.raftLog.lastSnapshotIndex); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.raftLog.lastSnapshotTerm); err != nil {
		rf.logger.Panic(err)
	}

	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	// Raft state.
	var (
		term int
		vote int
	)
	if decoder.Decode(&term) != nil || decoder.Decode(&vote) != nil {
		rf.logger.Panic("failed to decode raft state from persist data")
	}

	// Raft log.
	var (
		entries           []Entry
		committed         int
		applied           int
		lastSnapshotIndex int
		lastSnapshotTerm  int
	)
	if decoder.Decode(&entries) != nil || decoder.Decode(&committed) != nil || decoder.Decode(&applied) != nil ||
		decoder.Decode(&lastSnapshotIndex) != nil || decoder.Decode(&lastSnapshotTerm) != nil {
		rf.logger.Panic("failed to decode raft log from persist data")
	}

	// Recovery.
	rf.term = term
	rf.vote = vote
	rf.raftLog = NewRaftLog(entries, committed, applied, lastSnapshotIndex, lastSnapshotTerm)
}

// Start proposes a command in Raft cluster.
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill sets the peer to dead.
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed checks is the peer is killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// alive checks if the peer is alive.
func (rf *Raft) alive() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 0
}

// isStandalone checks if the peer is standalone.
func (rf *Raft) isStandalone() bool {
	return len(rf.peers) == 1
}

// lock tries to acquire lock with action log.
func (rf *Raft) lock(action string) {
	rf.logger.Debugf("%s try to lock for: %s", rf, action)
	rf.mu.Lock()
	rf.logger.Debugf("%s succeed to lock for: %s", rf, action)
}

// lock tries to release lock with action log.
func (rf *Raft) unlock(action string) {
	rf.mu.Unlock()
	rf.logger.Debugf("%s succeed to unlock after: %s", rf, action)
}

// becomeFollower transform this peer's state to Follower.
func (rf *Raft) becomeFollower(term int, lead int) {
	rf.logger.Infof("%s role: %s -> %s, current leader: %v", rf, rf.role, Follower, lead)
	rf.role = Follower
	rf.term = term
	rf.vote = None
	rf.lead = lead

	rf.persist()
	rf.tick.stopLogReplicationTicker()
	rf.tick.resetElectionTimeoutTicker()
}

// becomeCandidate transform this peer's role to Candidate.
func (rf *Raft) becomeCandidate() {
	rf.logger.Infof("%s role: %s -> %s, previous leader: %v", rf, rf.role, Candidate, rf.lead)
	rf.role = Candidate

	// Increment term.
	rf.term++
	rf.lead = None

	// Create a new ballot box.
	rf.ballotBox = make(map[int]bool)

	// Vote for itself.
	rf.vote = rf.id
	rf.ballotBox[rf.id] = true

	rf.persist()
	rf.tick.stopLogReplicationTicker()
	rf.tick.resetElectionTimeoutTicker()
}

// becomeLeader transform this peer's role to Leader.
func (rf *Raft) becomeLeader() {
	rf.logger.Infof("%s role: %s -> %s, previous leader: %v", rf, rf.role, Leader, rf.lead)
	rf.role = Leader
	rf.lead = rf.id
	rf.vote = None

	lastIndex := rf.raftLog.LastIndex()

	// Note: Leader should propose a noop entry on its term.
	rf.raftLog.AppendEntry(NewEntry(rf.term, lastIndex+1, nil))

	// Set replication progress.
	for peer := range rf.peers {
		if peer == rf.id {
			rf.progress[peer].Match = lastIndex + 1
			rf.progress[peer].Next = lastIndex + 2
		} else {
			rf.progress[peer].Next = lastIndex + 1
		}
	}

	// Replicate the noop entry immediately.
	if rf.isStandalone() {
		rf.raftLog.CommitTo(rf.progress[rf.id].Match)
	} else {
		go rf.replicateLog(true)
	}

	rf.persist()
	rf.tick.stopElectionTimeoutTicker()
	rf.tick.resetLogReplicationTicker()
}

// ticker triggers leader to broadcast heartbeat
// follower or candidate to start election.
func (rf *Raft) ticker() {
	for rf.alive() {
		select {
		case <-rf.tick.electionTimeoutTicker.C:
			rf.startElection()
		case <-rf.tick.logReplicationTicker.C:
			if rf.isLeader() {
				rf.logger.Infof("%s tick log", rf)
			}
			rf.replicateLog(true)
		}
	}
}

// applier listens the notify channel and do applying command.
func (rf *Raft) applier() {
	for rf.alive() {
		select {
		case <-rf.notifyApplyCh:
			rf.applyCommand()
		}
	}
}

// applyCommand sends commands to state machine by apply channel.
func (rf *Raft) applyCommand() {

}

// Make creates a Raft server.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.id = me
	rf.tick = newTicker()
	rf.raftLog = NewRaftLog(nil, Zero, Zero, Zero, Zero)
	rf.progress = make(map[int]*Progress)
	for peer := range peers {
		rf.progress[peer] = &Progress{}
	}

	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 10)
	rf.logger = log.NewZapLogger("Raft").Sugar()

	rf.becomeFollower(Zero, None)
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}
