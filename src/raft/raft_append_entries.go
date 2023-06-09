package raft

import (
	"fmt"
	"sort"
)

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

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("AEA{Term:%d LeaderId %d PrevLogIndex:%d PrevLogTerm:%d Entries:%+v LeaderCommit:%d}",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.Entries, a.LeaderCommit)
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

func (r AppendEntriesReply) String() string {
	return fmt.Sprintf("AER{Term:%d Success:%v LogIndex:%d LogTerm:%d}",
		r.Term, r.Success, r.LogIndex, r.LogTerm)
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
			rf.logger.Debugf("%s Replicate log to peer [%d], current entries: %+v", rf, peer, rf.raftLog.entries)
			go rf.sendAppendEntriesToPeer(peer)
		}
	}
}

// shouldSendAppendEntries checks if leader needs to send AppendEntriesArgs to the specified peer.
func (rf *Raft) shouldSendAppendEntries(isHeartbeat bool, l *RaftLog, prs *Progress) bool {
	return isHeartbeat || l.LastIndex() > prs.Next || l.committed > prs.Match
}

// sendAppendEntriesToPeer sends AppendEntriesArgs to the specified peer.
func (rf *Raft) sendAppendEntriesToPeer(peer int) {
	rf.mu.Lock()
	if !rf.isLeader() {
		rf.mu.Unlock()
		return
	}

	if rf.shouldSendInstallSnapshot(peer) {
		rf.mu.Unlock()
		go rf.sendInstallSnapshotToPeer(peer)
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
		rf.logger.Warnf("%s Failed to send AEA to peer [%d]", rf, peer)
		return
	}

	rf.handleAppendEntriesReply(peer, reply)
}

// handleAppendEntriesReply handles AppendEntriesReply from the specified peer.
func (rf *Raft) handleAppendEntriesReply(peer int, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return
	}

	if reply.Term > rf.term {
		rf.logger.Infof("%s AER term(%d) > current term(%d), become follower at term: %d",
			rf, reply.Term, rf.term, reply.Term)
		rf.becomeFollower(reply.Term, None)
		rf.persistState()
		return
	}

	if !reply.Success {
		l := rf.raftLog
		logIndex := reply.LogIndex
		logTerm := reply.LogTerm
		rf.logger.Infof("%s Handle conflict scene for peer [%d]: index = %d, term = %d",
			rf, peer, logIndex, logTerm)

		// Send snapshot to it.
		if logIndex < l.FirstIndex() {
			go rf.sendInstallSnapshotToPeer(peer)
			return
		}

		// Handle the conflict scene.
		if logTerm != Zero {
			// Rollback to the index of the first entry on its term.
			sliceIndex := sort.Search(l.Length(), func(i int) bool {
				return l.Get(i).Term > logTerm
			})
			entryIndex := l.ToEntryIndex(sliceIndex)
			rf.logger.Infof("%s Handle conflict log term for peer [%d]: %d, rollback next index to: %d",
				rf, peer, logTerm, entryIndex)

			if entryIndex > l.FirstIndex() && entryIndex <= l.LastIndex() && l.EntryAt(entryIndex).Term == logTerm {
				logIndex = entryIndex
			}
		}

		rf.progress[peer].Next = logIndex
		go rf.sendAppendEntriesToPeer(peer)
		return
	}

	rf.advanceProgress(peer, reply.LogIndex)
}

// advanceProgress advances the progress of the specified peer.
func (rf *Raft) advanceProgress(peer int, logIndex int) {
	prs := rf.progress[peer]
	prs.Match = min(logIndex, rf.raftLog.LastIndex())
	prs.Next = prs.Match + 1
	rf.logger.Debugf("%s Advance peer [%d] progress: %+v", rf, peer, prs)
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
		rf.persistState()
		rf.logger.Infof("%s Advance committed index to: %d", rf, committedQuorum)
		rf.notifyApplyCh <- applySignal

		// Sync committed immediately.
		go rf.replicateLog(false)
	}
}

// AppendEntries handles AppendEntriesArgs and replies AppendEntriesReply.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logger.Debugf("%s Receive %s from peer [%d]", rf, args, args.LeaderId)
	defer rf.logger.Debugf("%s Send %s to peer [%d]", rf, reply, args.LeaderId)

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
			rf.logger.Infof("%s Peer [%d] entry{Index=%d, Term=%d} is conflict with Leader entry{Index=%d, Term=%d}",
				rf, rf.id, args.PrevLogIndex, prevEntry.Term, args.PrevLogIndex, args.PrevLogTerm)
			reply.LogTerm = prevEntry.Term
			reply.LogIndex = sort.Search(l.ToSliceIndex(args.PrevLogIndex+1), func(i int) bool {
				return l.Get(i).Term == prevEntry.Term
			})
			return
		}
	}

	rf.logger.Debugf("%s Before AEA, last index: %d, entries: %+v", rf, l.LastIndex(), l.entries)
	defer func() {
		rf.logger.Debugf("%s After AEA, last index: %d, entries: %+v", rf, rf.raftLog.LastIndex(), rf.raftLog.entries)
	}()

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
				rf.logger.Debugf("%s Update entry: %+v -> %+v", rf, entry, newEnt)
				l.Update(entry.Index, newEnt)
				rf.logger.Debugf("%s Remove entry after: %+v", rf, entry)
				l.RemoveAfter(entry.Index)
			}
		} else {
			// (6) 4. Append any new entries not already in the log.
			l.AppendEntries(args.Entries[i:])
			rf.logger.Debugf("%s Append entries: %+v", rf, args.Entries[i:])
			break
		}
	}

	// (7) 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, lastIndex).
	if args.LeaderCommit > l.committed {
		committed := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		l.CommitTo(committed)
		rf.logger.Infof("%s Advance committed index to: %d", rf, committed)
		rf.notifyApplyCh <- applySignal
	}

	// (8) Handle corner case: multiple leaders in the cluster.
	if rf.isLeader() && args.LeaderCommit >= l.committed {
		rf.becomeFollower(args.Term, args.LeaderId)
	}

	rf.persistState()
	reply.Success = true
	reply.LogIndex = l.LastIndex()
}

// sendAppendEntries calls AppendEntries RPC.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
