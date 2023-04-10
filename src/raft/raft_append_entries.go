package raft

import "sort"

// @Author KHighness
// @Update 2023-04-08

// AppendEntriesArgs structure.
type AppendEntriesArgs struct {
	Term         int     // leader's Term
	LeaderId     int     // leader's id
	PrevLogIndex int     // the index of the previous entry, index = (nextIndex - 1)
	PrevLogTerm  int     // the term of the previous entry
	Entries      []Entry // the entries
	LeaderCommit int     // leader's commit index
}

// AppendEntriesReply structure.
type AppendEntriesReply struct {
	Term    int  // reply term
	Success bool // if follower append entries successfully

	// LogIndex maybe follower's commit index or follower's conflict index.
	//	If Success is true, it is follower's last index.
	//	If follower does not have the leader's previous entry,
	//	it is follower's last index incremented value.
	//	If the term of follower's entry is unmatched with the term of leader's previous entry,
	//	it is the index of follower's first conflict entry.
	LogIndex int
	// LogTerm is not a required field.
	//	If Success is true, it is the default value Zero.
	//	If the term of follower's entry is unmatched with the term of leader's previous entry,
	//	it is the term of follower's conflict entry.
	LogTerm int
}

// replicateLog does replicating log to other peers.
func (rf *Raft) replicateLog(isHeartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return
	}

	rf.logger.Infof("%s Replicate log", rf)
	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}

		rf.replicateLogToPeer(peer, isHeartbeat)
	}
}

// replicateLogToPeer does replicating log to the specified peer.
// If the peer's next index is greater than leader's last snapshot index, leader will send entries to it.
// If the peer's next index is less than leader's last snapshot index, leader will send snapshot to it.
func (rf *Raft) replicateLogToPeer(peer int, isHeartbeat bool) {
	prs := rf.progress[peer]
	if rf.shouldSendAppendEntries(isHeartbeat, rf.raftLog, prs) {
		if rf.shouldSendInstallSnapshot(peer) {
			go rf.sendInstallSnapshotToPeer(peer)
		} else {
			go rf.sendAppendEntriesToPeer(peer)
		}
	}
}

// shouldSendAppendEntries checks if leader need to send AppendEntriesArgs to the specified peer.
func (rf *Raft) shouldSendAppendEntries(isHeartbeat bool, l *RaftLog, prs *Progress) bool {
	return isHeartbeat || l.LastIndex() > prs.Next || l.committed > prs.Match
}

// sendAppendEntriesToPeer sends AppendEntriesArgs to the specified peer.
func (rf *Raft) sendAppendEntriesToPeer(peer int) {
	rf.mu.Lock()
	if !rf.isLeader() || rf.shouldSendInstallSnapshot(peer) {
		rf.mu.Unlock()
		return
	}

	prevEntry := rf.raftLog.EntryAt(rf.progress[peer].Next - 1)
	entries := rf.raftLog.EntriesAfter(prevEntry.Index)
	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.id,
		PrevLogIndex: prevEntry.Index,
		PrevLogTerm:  prevEntry.Term,
		Entries:      entries,
		LeaderCommit: rf.raftLog.committed,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peer, args, &reply) {
		rf.logger.Warnf("%s Failed to send AEA to peer %d", rf, peer)
		return
	}

	rf.logger.Debugf("%s Send AEA%+v to peer %d", rf, args, peer)
	rf.logger.Debugf("%s Receive AER%+v from peer %d", rf, reply, peer)
	rf.handleAppendEntriesReply(peer, reply)
}

// handleAppendEntriesReply handles AppendEntriesReply from the specified peer.
func (rf *Raft) handleAppendEntriesReply(peer int, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.term {
		rf.logger.Infof("%s AEA term(%d) > current term(%d), become follower at term: %d",
			rf, reply.Term, rf.term, reply.Term)
		rf.becomeFollower(reply.Term, None)
		return
	} else if reply.Term < rf.term {
		return
	}

	if !reply.Success {
		logIndex := reply.LogIndex
		logTerm := reply.Term
		// Handle the conflict scene.
		if reply.LogTerm != Zero {
			l := rf.raftLog
			sliceIndex := sort.Search(l.Length(), func(i int) bool {
				return l.EntryAt(i).Term > logTerm
			})
			entryIndex := l.ToEntryIndex(sliceIndex)
			if entryIndex >= l.FirstIndex() && l.EntryAt(entryIndex).Term == logTerm {
				logIndex = entryIndex
			} else {
				go rf.sendInstallSnapshotToPeer(peer)
			}
		}
		rf.progress[peer].Next = logIndex
		// TODO(Khighness): send immediately
		go rf.sendAppendEntriesToPeer(peer)
		return
	}

	rf.progress[peer].Match = reply.LogIndex
	rf.progress[peer].Next = reply.LogIndex + 1
	rf.tryAdvanceCommitted()
}

// tryAdvanceCommitted tries to advance the committed index.
func (rf *Raft) tryAdvanceCommitted() {
	total := len(rf.peers)
	i := 0
	match := make(intSlice, total)
	for _, prs := range rf.progress {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)

	committedQuorum := match[(total-1)/2]
	l := rf.raftLog
	if committedQuorum > l.committed && l.EntryAt(committedQuorum).Term == rf.term {
		rf.raftLog.CommitTo(committedQuorum)
		rf.logger.Infof("%s Advance committed index to: %d", rf, committedQuorum)

		// Sync committed immediately.
		go rf.replicateLog(false)
		return
	}
}

// AppendEntries handles AppendEntriesArgs and replies AppendEntriesReply.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logger.Infof("%s Receive AEA%+v from peer %d", rf, args, args.LeaderId)
	defer rf.logger.Infof("%s Send AER%+v to peer %d", rf, reply, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Success = false

	// (1) 1. Check leader's term.
	if args.Term < rf.term {
		return
	}

	// (2) For all roles: if Term > rf.term, convert to follower.
	if args.Term > rf.term {
		rf.becomeFollower(args.Term, args.LeaderId)
		reply.Term = rf.term
	}
	rf.tick.resetElectionTimeoutTicker()

	// (3) 2. Check if log contains leader's previous entry.
	l := rf.raftLog
	lastIndex := l.LastIndex()
	if args.PrevLogIndex > lastIndex {
		reply.LogIndex = lastIndex + 1
		return
	}

	// (4) Optimization: search the first entry whose term is unmatched with leader's previous entry.
	if args.PrevLogIndex > l.FirstIndex() {
		prevEntry := l.EntryAt(args.PrevLogIndex)
		if prevEntry.Term != args.PrevLogTerm {
			reply.LogTerm = prevEntry.Term
			reply.LogIndex = sort.Search(l.ToSliceIndex(args.PrevLogIndex+1), func(i int) bool {
				return l.EntryAt(i).Term == prevEntry.Term
			})
		}
	}

	// Append leader's new entries.
	for i, newEnt := range args.Entries {
		if newEnt.Index < l.FirstIndex() {
			continue
		}
		if newEnt.Index <= l.LastIndex() {
			// (5) 3. If an existing entry conflicts with a new one (same index but different terms),
			// update the existing entry and delete all entries that follow it.
			entry := l.EntryAt(newEnt.Index)
			if entry.Term != newEnt.Term {
				l.Update(entry.Index, newEnt)
				l.RemoveAfter(entry.Index)
			}
		} else {
			// (6) 4. Append any new entries not already in the log.
			l.AppendEntries(args.Entries[i:])
			break
		}
	}

	// (7) 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, lastIndex).
	if args.LeaderCommit > l.committed {
		l.CommitTo(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
	}

	reply.Success = true
	reply.LogIndex = l.LastIndex()
}

// sendAppendEntries calls AppendEntries RPC.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
