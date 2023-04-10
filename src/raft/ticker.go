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

// resetLogReplicationTicker resets logReplicationTicker.
func (t *ticker) resetLogReplicationTicker() {
	t.logReplicationTicker.Reset(logReplicationInterval)
}

// stopLogReplicationTicker stopes logReplicationTicker.
func (t *ticker) stopLogReplicationTicker() {
	t.logReplicationTicker.Stop()
}

// resetElectionTimeoutTicker reset electionTimeoutTicker.
func (t *ticker) resetElectionTimeoutTicker() {
	electionTimeout := randTimeDuration(electionTimeoutLower, electionTimeoutUpper)
	t.electionTimeoutTicker.Reset(electionTimeout)
}

// stopElectionTimeoutTicker stopes electionTimeoutTicker.
func (t *ticker) stopElectionTimeoutTicker() {
	t.electionTimeoutTicker.Stop()
}
