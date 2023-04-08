package raft

import (
	"time"
)

// @Author KHighness
// @Update 2023-04-08

const (
	logReplicationInterval = time.Duration(150) * time.Millisecond
	electionTimeoutLower   = time.Duration(300) * time.Millisecond
	electionTimeoutUpper   = time.Duration(600) * time.Millisecond
)

// ticker contains all tickers for Raft.
// It does not need to be persisted in Raft.
type ticker struct {
	logReplicationTicker  *time.Ticker // for Leader
	electionTimeoutTicker *time.Ticker // for Follower, Candidate
}

// newTicker creates a ticker instance internally.
func newTicker() ticker {
	return ticker{
		logReplicationTicker:  time.NewTicker(logReplicationInterval),
		electionTimeoutTicker: time.NewTicker(randTimeDuration(electionTimeoutLower, electionTimeoutUpper)),
	}
}

// resetLogReplicationTicker used to notify Leader to broadcast heartbeat periodically.
// The call principle is:
//	For leader, it is called after broadcasting a round of heartbeat.
func (t *ticker) resetLogReplicationTicker() {
	t.logReplicationTicker.Reset(logReplicationInterval)
}

// resetElectionTimeoutTicker used to prevent Follower or Candidate starting election.
// The call principle is:
//	For follower or candidate, it is called after receiving Leader's heartbeat
//	or Candidate's request vote.
//	After starting an round of election, it should also be called.
func (t *ticker) resetElectionTimeoutTicker() {
	electionTimeout := randTimeDuration(electionTimeoutLower, electionTimeoutUpper)
	t.electionTimeoutTicker.Reset(electionTimeout)
}
