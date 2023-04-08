package raft

// @Author KHighness
// @Update 2023-04-08

// AppendEntriesArgs structure.
type AppendEntriesArgs struct {
	Term         int     // leader's Term
	PrevLogIndex int     // the previous index of the last entry
	PrevLogTerm  int     // the previous term of the last entry
	Entries      []Entry // the entries
	LeaderCommit int     // leader's commit index
}

// AppendEntriesReply structure.
type AppendEntriesReply struct {
	Term          int  // reply term
	Success       bool // if accepted
	ConflictIndex int  // the index of the first conflict entry
	ConflictTerm  int  // the term of the first conflict entry
}

// replicateLog does replicating log to other nodes.
func (rf *Raft) replicateLog(isHeartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return
	}

	lastEntry := rf.raftLog.LastEntry()
	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}

		prs := rf.progress[peer]
		if isHeartbeat || lastEntry.Index >= prs.Next {
			prevEntry := rf.raftLog.EntryAt(prs.Next - 1)

			if prevEntry.Index < rf.raftLog.lastSnapshotIndex {
				go rf.sendInstallSnapshotToPeer(peer)
			} else {
				entries := rf.raftLog.EntriesAfter(prevEntry.Index)
				args := &AppendEntriesArgs{
					Term:         rf.term,
					PrevLogIndex: prevEntry.Index,
					PrevLogTerm:  prevEntry.Term,
					Entries:      entries,
					LeaderCommit: rf.raftLog.committed,
				}
				go rf.sendAppendEntriesToPeer(peer, args)
			}
		}
	}
}

// sendAppendEntriesToPeer sends AppendEntriesArgs to the specified peer.
func (rf *Raft) sendAppendEntriesToPeer(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peer, args, &reply) {
		rf.logger.Warnf("%s failed to send AEA to: %d", rf, peer)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
}

// tryAdvanceCommitted tries to advance the committed index.
func (rf *Raft) tryAdvanceCommitted() {

}

// AppendEntries handles AppendEntriesArgs and replies AppendEntriesReply.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Lock()

	// 1. Check leader's term

}

// sendAppendEntries calls AppendEntries RPC.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
