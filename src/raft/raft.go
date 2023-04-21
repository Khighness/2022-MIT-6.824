package raft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/log"

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

// enableLockLog controls whether to print the lock hook log.
const enableLockLog = false

// Progress structure.
// It represents a follower's progress in the view of the leader.
type Progress struct {
	Match, Next int
}

// applySignal is a signal to notify leader to command.
var applySignal = struct{}{}

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
	raftLog *RaftLog // Raft log

	ballotBox map[int]bool      // Ballot box
	progress  map[int]*Progress // Follower's log replication progress

	// applyCh is a channel to deliver ApplyMsg.
	applyCh chan ApplyMsg // Used to send ApplyMsg to state machine
	// notifyApplyCh should be set to a relatively large value to avoid blocking.
	notifyApplyCh chan struct{} // Used to notify peer deliver command

	logger *zap.SugaredLogger
}

// String uses for easy logging.
// It looks like:
//	[role name - peer id : current term]
func (rf *Raft) String() string {
	return fmt.Sprintf("[%s-%v:%d]", rf.role.ShortString(), rf.id, rf.term)
}

// lock tries to acquire lock with action log.
func (rf *Raft) lock() {
	if enableLockLog {
		action := getCalledFunction()
		rf.logger.Infof("%s Try to lock for: %s", rf, action)
		defer rf.logger.Infof("%s Succeed to lock for: %s", rf, action)
	}
	rf.mu.Lock()
}

// lock tries to release lock with action log.
func (rf *Raft) unlock() {
	rf.mu.Unlock()
	if enableLockLog {
		rf.logger.Infof("%s Succeed to unlock after: %s", rf, getCalledFunction())
	}
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

// persistState saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persistState() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	var err error
	if err = encoder.Encode(rf.term); err != nil {
		rf.logger.Panic(err)
	}
	if err = encoder.Encode(rf.vote); err != nil {
		rf.logger.Panic(err)
	}
	if err = rf.raftLog.Encode(encoder); err != nil {
		rf.logger.Panic(err)
	}

	rf.persister.SaveRaftState(buffer.Bytes())
	rf.logger.Debugf("%s Persist state, term = %d, vote = %d, log = %s", rf, rf.term, rf.vote, rf.raftLog)
}

// persistStateAndSnapshot saves Raft's state and snapshot to stable storage.
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	rf.persistState()
	rf.persister.SaveSnapshot(snapshot)
}

// readPersist restores previously persisted state and snapshot.
func (rf *Raft) readPersist(state []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if state == nil || len(state) < 1 {
		return
	}

	// Recover state.
	statBuffer := bytes.NewBuffer(state)
	statDecoder := labgob.NewDecoder(statBuffer)

	var (
		term int
		vote int
	)
	if statDecoder.Decode(&term) != nil || statDecoder.Decode(&vote) != nil {
		rf.logger.Panic("failed to decode raft state from persist data")
	}
	rf.term = term
	rf.vote = vote

	// Recover log.
	rf.raftLog = NewRaftLogFromDecoder(statDecoder)

	// Peer maybe need to apply entries.
	rf.notifyApplyCh <- applySignal

	rf.logger.Infof("%s Recover state, term = %d, vote = %d, log = %s", rf, rf.term, rf.vote, rf.raftLog)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return None, None, false
	}

	entry := rf.leaderAppendEntry(command)
	rf.logger.Infof("%s Propose entry: %+v", rf, entry)
	go rf.replicateLog(false)
	return entry.Index, entry.Term, true
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
	rf.tick.stopElectionTimeoutTicker()
	rf.tick.stopLogReplicationTicker()
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

// becomeFollower transforms this peer's role to Follower.
func (rf *Raft) becomeFollower(term int, lead int) {
	rf.logger.Infof("%s Role: %s -> %s, current leader: %v", rf, rf.role, Follower, lead)
	rf.role = Follower
	rf.term = term
	rf.vote = None
	rf.lead = lead

	rf.tick.stopLogReplicationTicker()
	rf.tick.resetElectionTimeoutTicker()
}

// becomeCandidate transforms this peer's role to Candidate.
func (rf *Raft) becomeCandidate() {
	rf.logger.Infof("%s Role: %s -> %s, previous leader: %v", rf, rf.role, Candidate, rf.lead)
	rf.role = Candidate

	// Increment term.
	rf.term++
	rf.lead = None

	// Create a new ballot box.
	rf.ballotBox = make(map[int]bool)

	// Vote for itself.
	rf.vote = rf.id
	rf.ballotBox[rf.id] = true

	rf.tick.stopLogReplicationTicker()
	rf.tick.resetElectionTimeoutTicker()
}

// becomeLeader transforms this peer's role to Leader.
func (rf *Raft) becomeLeader() {
	rf.logger.Infof("%s Role: %s -> %s, previous leader: %v", rf, rf.role, Leader, rf.lead)
	rf.role = Leader
	rf.lead = rf.id
	rf.vote = None

	// Note: Leader maybe add a no-operation log entry on its term.
	// But don't do this in MIT6.824, otherwise you can't pass lab2B.
	// noopEntry := rf.leaderAppendEntry(nil)

	// Initialize replication progress.
	nextIndex := rf.raftLog.LastIndex() + 1
	for peer := range rf.peers {
		if peer != rf.id {
			rf.progress[peer].Next = nextIndex
		}
	}

	// Broadcast heartbeat immediately。
	go rf.replicateLog(true)

	rf.tick.stopElectionTimeoutTicker()
	rf.tick.resetLogReplicationTicker()
}

// ticker triggers leader to broadcast heartbeat
// and follower or candidate to start election.
func (rf *Raft) ticker() {
	for rf.alive() {
		select {
		case <-rf.tick.electionTimeoutTicker.C:
			rf.startElection()
		case <-rf.tick.logReplicationTicker.C:
			rf.replicateLog(true)
		}
	}
}

// applier listens the notifyApplyCh and do applying command.
func (rf *Raft) applier() {
	for rf.alive() {
		select {
		case <-rf.notifyApplyCh:
			rf.applyCommand()
		}
	}
}

// applyCommand sends commands to state machine by applyCh.
func (rf *Raft) applyCommand() {
	rf.mu.Lock()

	l := rf.raftLog
	if l.applied < l.committed {
		for idx := l.applied + 1; idx <= l.committed; idx++ {
			entry := l.EntryAt(idx)
			rf.mu.Unlock()

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg

			rf.mu.Lock()
			l.ApplyTo(idx)
			rf.logger.Infof("%s Apply entry: %+v", rf, entry)
		}
	}

	rf.mu.Unlock()
}

// leaderAppendEntry appends an entry to leader's log.
func (rf *Raft) leaderAppendEntry(command interface{}) Entry {
	entryIndex := rf.raftLog.LastIndex() + 1
	entry := NewEntry(rf.term, entryIndex, command)
	rf.raftLog.AppendEntry(entry)
	rf.persistState()

	rf.progress[rf.id].Match = entryIndex
	rf.progress[rf.id].Next = entryIndex + 1
	return entry
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
	timeStart := time.Now()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.id = me
	rf.role = Follower
	rf.term = Zero
	rf.vote = Zero
	rf.lead = None
	rf.tick = newTicker()
	rf.raftLog = NewRaftLog(nil, Zero, Zero, Zero, Zero)
	rf.progress = make(map[int]*Progress)
	for peer := range peers {
		rf.progress[peer] = &Progress{}
	}

	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 300)
	rf.logger = log.NewZapLogger("Raft").Sugar()
	rf.logger.Infof("Start peer [%d] in raft cluster: %v", me, peers)

	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.applier()

	rf.logger.Infof("%s Raft peer [%d] initializes successfully in %vms",
		rf, me, time.Since(timeStart).Milliseconds())
	return rf
}
