package raft

import "fmt"

// @Author KHighness
// @Update 2023-04-08

// InstallSnapshotArgs structure.
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // leader's id
	LastIncludedIndex int    // the index of the last entry included in snapshot
	LastIncludedTerm  int    // the term of the last entry included in snapshot
	Data              []byte // snapshot's data
}

func (a InstallSnapshotArgs) String() string {
	return fmt.Sprintf("ISA{Term:%d LeaderId:%d LastIncludedIndex:%d LastIncludedTerm:%d}",
		a.Term, a.LeaderId, a.LastIncludedIndex, a.LastIncludedTerm)
}

// InstallSnapshotReply structure.
type InstallSnapshotReply struct {
	Term int // reply term
}

func (a InstallSnapshotReply) String() string {
	return fmt.Sprintf("ISR{Term:%d}", a.Term)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	if !rf.isLastIncludedIndexValid(lastIncludedIndex) {
		rf.logger.Infof("%s CondInstallSnapshot, log: %s, invalid index: %d", rf, rf.raftLog, lastIncludedIndex)
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	rf.doApplyInstallSnapshot(lastIncludedTerm, lastIncludedIndex, snapshot)
	return true
}

// isLastIncludedIndexValid checks if the lastIncludedIndex is valid.
func (rf *Raft) isLastIncludedIndexValid(lastIncludedIndex int) bool {
	l := rf.raftLog
	return lastIncludedIndex >= l.lastIncludedIndex && // The lastIncludedIndex can not be less than the old one.
		lastIncludedIndex >= l.committed && // The lastIncludedIndex can not be less the committed index.
		l.committed == l.applied // All the committed entries must been applied.
}

// sendInstallSnapshotToPeer sends InstallSnapshotArgs to the specified peer.
func (rf *Raft) sendInstallSnapshotToPeer(peer int) {
	rf.mu.Lock()
	if !rf.isLeader() {
		rf.mu.Unlock()
		return
	}

	l := rf.raftLog
	args := &InstallSnapshotArgs{
		Term:              rf.term,
		LeaderId:          rf.id,
		LastIncludedIndex: l.lastIncludedIndex,
		LastIncludedTerm:  l.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if !rf.SendInstallSnapshot(peer, args, &reply) {
		rf.logger.Warnf("%s Failed to send ISA to peer [%d]", rf, peer)
		return
	}

	rf.handleInstallSnapshotReply(peer, args, reply)
}

// shouldSendInstallSnapshot checks if leader need to send InstallSnapshotArgs to the given peer.
func (rf *Raft) shouldSendInstallSnapshot(peer int) bool {
	return rf.progress[peer].Next-1 < rf.raftLog.FirstIndex()
}

// handleInstallSnapshotReply handles InstallSnapshotReply from the specified peer.
func (rf *Raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return
	}

	if reply.Term > rf.term {
		rf.logger.Infof("%s ISR term(%d) > current term(%d), become follower at term: %d",
			rf, reply.Term, rf.term, reply.Term)
		rf.becomeFollower(reply.Term, None)
		return
	}

	if args.Term != rf.term || args.LastIncludedIndex != rf.raftLog.FirstIndex() {
		rf.logger.Infof("%s Raft state has changed, ignore %s from peer [%d]", rf, reply, peer)
		return
	}

	rf.advanceProgress(peer, args.LastIncludedIndex)
}

// InstallSnapshot handles InstallSnapshotArgs and replies InstallSnapshotReply.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.logger.Debugf("%s Receive %s from peer [%d]", rf, args, args.LeaderId)
	defer rf.logger.Debugf("%s Send %s to peer [%d]", rf, reply, args.LeaderId)

	rf.mu.Lock()
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.mu.Unlock()
		return
	} else if args.Term > rf.term {
		rf.becomeFollower(args.Term, args.LeaderId)
		reply.Term = rf.term
	}
	rf.tick.resetElectionTimeoutTicker()

	if !rf.isLastIncludedIndexValid(args.LastIncludedIndex) {
		rf.logger.Infof("%s InstallSnapshot, log: %s, invalid index: %d", rf, rf.raftLog, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	rf.doApplyInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, args.Data)
}

// doApplyInstallSnapshot does applying snapshot.
// It is called by CondInstallSnapshot or InstallSnapshot.
func (rf *Raft) doApplyInstallSnapshot(lastSnapshotTerm int, lastSnapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	l := rf.raftLog
	if lastSnapshotIndex <= l.FirstIndex() {
		rf.mu.Unlock()
		return
	}

	rf.raftLog = l.Apply(lastSnapshotIndex, lastSnapshotTerm)
	rf.persistStateAndSnapshot(snapshot)
	rf.logger.Infof("%s Apply snapshot, old log: %s, new log: %s", rf, l, rf.raftLog)
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  lastSnapshotTerm,
		SnapshotIndex: lastSnapshotIndex,
	}
	rf.applyCh <- applyMsg
	rf.logger.Infof("%s Apply snapshot: [lastSnapshotTerm=%d, lastSnapshotIndex=%d]",
		rf, lastSnapshotTerm, lastSnapshotIndex)
}

// Snapshot is a proactive action of the upper level (eg. kv server).
// Just remove the entries that have already been compacted into snapshot
// and save the given snapshot data.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.logger.Debugf("%s Snapshot is called, index: %d, snapshot: %d", rf, index, snapshot)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	l := rf.raftLog
	if index <= l.FirstIndex() || index > l.LastIndex() {
		rf.logger.Warnf("%s Snapshot: snapshot index(%d) is out bound of (%d, %d], ignore",
			rf, index, l.FirstIndex(), l.LastIndex())
		return
	}

	l.CompactTo(index)
	rf.persistStateAndSnapshot(snapshot)
	rf.logger.Infof("%s Snapshot, log: %s", rf, l)
}

// SendInstallSnapshot calls InstallSnapshot RPC.
func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
