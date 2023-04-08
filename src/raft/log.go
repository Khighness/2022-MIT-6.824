package raft

import "go.uber.org/zap"

// @Author KHighness
// @Update 2023-04-08

// Entry structure.
type Entry struct {
	Term  int
	Index int
	Data  interface{}
}

// NewEntry creates an Entry instance.
func NewEntry(term, index int, data interface{}) Entry {
	return Entry{
		Term:  term,
		Index: index,
		Data:  data,
	}
}

// RaftLog structure.
// It must be persisted in Raft.
type RaftLog struct {
	entries []Entry

	committed int
	applied   int

	lastSnapshotTerm  int
	lastSnapshotIndex int

	logger *zap.SugaredLogger
}

// NewRaftLog creates a RaftLog instance.
func NewRaftLog(entries []Entry, committed, applied int, lastSnapshotTerm, lastSnapshotIndex int) *RaftLog {
	raftLog := &RaftLog{
		entries:           entries,
		committed:         committed,
		applied:           applied,
		lastSnapshotTerm:  lastSnapshotTerm,
		lastSnapshotIndex: lastSnapshotIndex,
		logger:            zap.S(),
	}

	if len(entries) == 0 { // dummy entry which stores the meta data of last snapshot
		raftLog.entries = []Entry{{
			Term:  lastSnapshotTerm,
			Index: lastSnapshotIndex,
		}}
	}

	return raftLog
}

// LastEntry returns the index of the last log entry.
func (l *RaftLog) LastIndex() int {
	return l.entries[len(l.entries)-1].Index
}

// LastEntry returns the last entry.
func (l *RaftLog) LastEntry() Entry {
	return l.entries[len(l.entries)-1]
}

// AppendEntry appends an entry to tail.
func (l *RaftLog) AppendEntry(entry Entry) {
	l.entries = append(l.entries, entry)
}

// EntryAt returns the entry corresponding to the index of log entry.
func (l *RaftLog) EntryAt(logIndex int) Entry {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	return l.entries[sliceIndex]
}

// After returns all the entries after the given index of log entry.
func (l *RaftLog) EntriesAfter(logIndex int) []Entry {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	return l.entries[sliceIndex+1:]
}

// CommitTo advances committed index.
func (l *RaftLog) CommitTo(committed int) {
	l.committed = committed
}

// ApplyTo advances applied index.
func (l *RaftLog) ApplyTo(applied int) {
	l.applied = applied
}

// toEntryIndex returns the index of log entry corresponding to the slice index.
func (l *RaftLog) ToEntryIndex(sliceIndex int) int {
	return l.entries[0].Index + sliceIndex
}

// toSliceIndex return the slice index in corresponding to the index of log entry.
func (l *RaftLog) ToSliceIndex(logIndex int) int {
	return logIndex - l.entries[0].Index
}

// Compact removes the entries that have been compacted into snapshots.
func (l *RaftLog) Compact(index int) {

}

// validateIndex validates the slice index.
func (l *RaftLog) validateIndex(sliceIndex int) {
	entLen := len(l.entries)
	if sliceIndex < 0 || sliceIndex >= entLen {
		l.logger.Panicf("RaftLog: index %s is out bound of [0, %d]", sliceIndex, entLen)
	}
}
