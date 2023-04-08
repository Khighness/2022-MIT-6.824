package raft

// @Author KHighness
// @Update 2023-04-08

// RequestVoteArgs structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // the index of the last entry
	LastLogTerm  int // the term of the last entry
}

// RequestVoteReply structure.
type RequestVoteReply struct {
	Term  int  // reply term
	Voted bool // if vote to candidate
}

// startElection starts a new round of election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.isLeader() {
		rf.mu.Unlock()
		return
	}

	rf.logger.Infof("%s start election", rf)
	rf.becomeCandidate()

	if rf.isStandalone() {
		rf.logger.Infof("%s run in standalone mode, become leader at %d directly", rf, rf.term)
		rf.becomeLeader()
		rf.mu.Unlock()
		return
	}

	lastEntry := rf.raftLog.LastEntry()
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.id,
		LastLogIndex: lastEntry.Index,
		LastLogTerm:  lastEntry.Term,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go rf.sendRequestVoteToPeer(peer, args)
	}
}

// sendRequestVoteToPeer sends RequestVoteArgs to the specified peer.
func (rf *Raft) sendRequestVoteToPeer(peer int, args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isCandidate() {
		return
	}

	var reply RequestVoteReply
	if !rf.sendRequestVote(peer, args, &reply) {
		rf.logger.Warnf("%s failed to send RVA to: %d", rf, peer)
		return
	}

	if rf.term < reply.Term {
		rf.becomeFollower(reply.Term, None)
		return
	}

	rf.ballotBox[peer] = reply.Voted
	total := len(rf.peers)
	quorum := len(rf.peers) / 2
	votes := len(rf.ballotBox)
	grantVotes := rf.grantVotes()
	if grantVotes > quorum {
		rf.logger.Infof("%s receive quorum votes: (%d/%d), become leader at term: %d",
			rf, grantVotes, votes, rf.term)
		rf.becomeLeader()
	} else if total == votes && grantVotes < total-quorum { // This round of election ended in failure.
		rf.logger.Infof("%s receive insufficient votes: (%d/%d), become follower at term: %d",
			rf, grantVotes, votes, rf.term-1)
		rf.becomeFollower(rf.term-1, None)
	}
}

// grantVotes return the count of grant vote that the peer receives.
func (rf *Raft) grantVotes() int {
	votes := 0
	for _, voted := range rf.ballotBox {
		if voted {
			votes++
		}
	}
	return votes
}

// RequestVote handles RequestVoteArgs and replies RequestVoteReply.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Voted = false

	// 1. Check if candidate's term is less than the receiver term.
	if args.Term < rf.term {
		return
	}

	// 2. Check if the receiver's vote is null.
	if rf.vote != None {
		return
	}

	// 3. Check if candidate's log is at least as up-to-date as receiver's log.
	lastEntry := rf.raftLog.LastEntry()
	if lastEntry.Term > args.LastLogTerm ||
		(lastEntry.Term == args.LastLogTerm && lastEntry.Index > args.LastLogIndex) {
		return
	}

	reply.Voted = true
}

// sendRequestVote calls RequestVote RPC.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
