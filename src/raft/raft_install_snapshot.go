package raft

// @Author KHighness
// @Update 2023-04-08

// InstallSnapshotArgs structure.
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // leader's id
	LastIncludedIndex int    // the index of the last entry in snapshot
	LastIncludedTerm  int    // the term of the last entry in snapshot
	Data              []byte // snapshot's data
}

// InstallSnapshotReply structure.
type InstallSnapshotReply struct {
	Term int // reply term
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()

	l := rf.raftLog
	if lastIncludedIndex <= l.FirstIndex() {
		rf.logger.Infof("%r CondInstallSnapshot: snapshot index(%d) <= current snapshot index(%d), ignore",
			rf, lastIncludedIndex, l.FirstIndex())
		rf.mu.Unlock()
		return false
	}

	l.Apply(lastIncludedIndex, lastIncludedTerm)
	rf.persistStateAndSnapshot(snapshot)
	rf.logger.Infof("%s InstallSnapshot, Log: %s", rf, l)
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}
	rf.applyCh <- applyMsg
	rf.logger.Infof("%s Apply snapshot: %+v", rf, applyMsg)
	return true
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
		LastIncludedIndex: l.lastSnapshotIndex,
		LastIncludedTerm:  l.lastSnapshotTerm,
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
	prevLogIndex := rf.progress[peer].Match
	return prevLogIndex < rf.raftLog.FirstIndex()
}

// handleInstallSnapshotReply handles InstallSnapshotReply from the specified peer.
func (rf *Raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Lock()

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
		rf.logger.Infof("%s Raft state has changed, ignore ISR%+v from peer [%d]", rf, reply, peer)
		return
	}

	rf.advanceProgress(peer, args.LastIncludedIndex)
}

// InstallSnapshot handles InstallSnapshotArgs and replies InstallSnapshotReply.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.logger.Debugf("%s Receive ISA%+v from peer [%d]", rf, args, args.LeaderId)
	defer rf.logger.Debugf("%s Send ISR%+v to peer [%d]", rf, reply, args.LeaderId)

	rf.mu.Lock()

	if args.Term > rf.term {
		rf.becomeFollower(args.Term, args.LeaderId)
	}
	reply.Term = rf.term
	rf.tick.resetElectionTimeoutTicker()

	l := rf.raftLog
	if args.LastIncludedIndex <= l.FirstIndex() {
		rf.logger.Infof("%r Receive ISA snapshot index(%d) <= current snapshot index(%d). ignore",
			rf, args.LastIncludedIndex, l.FirstIndex())
		rf.mu.Unlock()
		return
	}

	l.Apply(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persistStateAndSnapshot(rf.persister.ReadSnapshot())
	rf.logger.Infof("%s InstallSnapshot, Log: %s", rf, l)
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- applyMsg
	rf.logger.Infof("%s Apply snapshot: %+v", rf, applyMsg)
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
		rf.logger.Infof("%s Snapshot: snapshot index(%d) is out bound of (%d, %d], ignore",
			rf, index, l.FirstIndex(), l.LastIndex())
		return
	}

	l.Compact(index)
	rf.persistStateAndSnapshot(snapshot)
	rf.logger.Infof("%s Snapshot, Log: %s", rf, l)
}

// SendInstallSnapshot calls InstallSnapshot RPC.
func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
