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
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.isLeader() {
		return
	}

	rf.logger.Infof("%s Start election", rf)
	rf.becomeCandidate()

	if rf.isStandalone() {
		rf.logger.Infof("%s Run in standalone mode, become leader at %d directly", rf, rf.term)
		rf.becomeLeader()
		return
	}

	lastEntry := rf.raftLog.LastEntry()
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.id,
		LastLogIndex: lastEntry.Index,
		LastLogTerm:  lastEntry.Term,
	}

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
	if !rf.isCandidate() {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	if !rf.sendRequestVote(peer, args, &reply) {
		rf.logger.Warnf("%s Failed to send RVA to peer [%d]", rf, peer)
		return
	}

	rf.handleRequestVotesReply(peer, reply)
}

// handleRequestVotesReply handles RequestVoteReply from the specified peer.
func (rf *Raft) handleRequestVotesReply(peer int, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !rf.isCandidate() {
		return
	}

	if rf.term < reply.Term {
		rf.logger.Infof("%s RVR term(%d) > current term(%d), become follower at term: %d",
			rf, reply.Term, rf.term, reply.Term)
		rf.becomeFollower(reply.Term, None)
		return
	}

	rf.ballotBox[peer] = reply.Voted
	total := len(rf.peers)
	quorum := len(rf.peers) / 2
	votes := len(rf.ballotBox)
	grantVotes := rf.grantVotes()
	if grantVotes > quorum {
		rf.logger.Infof("%s Receive quorum votes: %d/%d, become leader at term: %d",
			rf, grantVotes, votes, rf.term)
		rf.becomeLeader()
	} else if total == votes && grantVotes < total-quorum { // This round of election ended in failure.
		rf.logger.Infof("%s Receive insufficient votes: %d/%d, become follower at term: %d",
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
	rf.logger.Debugf("%s Receive RVA%+v from peer [%d]", rf, args, args.CandidateId)
	defer rf.logger.Debugf("%s Send RVR%+v to peer [%d]", rf, reply, args.CandidateId)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.term
	reply.Voted = false

	// 1. Check if candidate's term is less than the receiver term
	//    or peer already grants vote to another candidate.
	if args.Term < rf.term ||
		(args.Term == rf.term && rf.vote != None && rf.vote != args.CandidateId) {
		return
	}

	// 2. For all roles: if Term > rf.term, convert to follower.
	if args.Term > rf.term {
		rf.logger.Infof("%s RVA term(%d) > current term(%d), become follower at term: %d",
			rf, args.Term, rf.term, reply.Term)
		rf.becomeFollower(args.Term, None)
		reply.Term = rf.term
	}

	// 3. Check if candidate's log is at least as up-to-date as receiver's log.
	lastEntry := rf.raftLog.LastEntry()
	if lastEntry.Term > args.LastLogTerm ||
		(lastEntry.Term == args.LastLogTerm && lastEntry.Index > args.LastLogIndex) {
		return
	}

	rf.vote = args.CandidateId
	reply.Voted = true
	rf.tick.resetElectionTimeoutTicker()
}

// sendRequestVote calls RequestVote RPC.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
