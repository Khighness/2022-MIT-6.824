package raft

import "sort"

// @Author KHighness
// @Update 2023-04-08

// AppendEntriesArgs structure.
type AppendEntriesArgs struct {
	Term         int     // leader's Term
	LeaderId     int     // leaders id
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
					LeaderId:     rf.id,
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

	reply.Term = rf.term
	reply.Success = false

	// (1) 1. Check leader's term.
	if args.Term < rf.term {
		return
	}

	// (2) For all roles: Term > rf.term: convert to follower.
	if args.Term > rf.term {
		rf.becomeFollower(args.Term, args.LeaderId)
	}

	// (3) 2. Check leader's prev log entry.
	l := rf.raftLog
	lastIndex := l.LastIndex()
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	// (4) Optimization: search the first entry whose term is unmatched with leader's prev log entry.
	if args.PrevLogIndex > l.FirstIndex() {
		prevEntry := l.EntryAt(args.PrevLogIndex)
		if prevEntry.Term != args.PrevLogTerm {
			reply.ConflictTerm = prevEntry.Term
			reply.ConflictIndex = sort.Search(l.ToSliceIndex(args.PrevLogIndex+1), func(i int) bool {
				return l.EntryAt(i).Term == prevEntry.Term
			})
		}
	}

	// Append leader's new log entries.
	reply.Success = true
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
		}
	}

	// (7) 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, lastIndex).
	if args.LeaderCommit > l.committed {
		l.CommitTo(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
	}
}

// sendAppendEntries calls AppendEntries RPC.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
