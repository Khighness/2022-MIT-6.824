package raft

import (
	"6.824/log"
	"go.uber.org/zap"
)

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
		logger:            log.NewZapLogger("RaftLog").Sugar(),
	}

	if len(entries) == 0 { // dummy entry which stores the meta data of last snapshot
		raftLog.entries = []Entry{{
			Term:  lastSnapshotTerm,
			Index: lastSnapshotIndex,
		}}
	}

	return raftLog
}

// FirstIndex returns the index of the first entry.
func (l *RaftLog) FirstIndex() int {
	return l.entries[0].Index + 1
}

// LastIndex returns the index of the last entry.
func (l *RaftLog) LastIndex() int {
	return l.entries[len(l.entries)-1].Index
}

// Length return the length of entries.
func (l *RaftLog) Length() int {
	return len(l.entries)
}

// LastEntry returns the last entry.
func (l *RaftLog) LastEntry() Entry {
	return l.entries[len(l.entries)-1]
}

// AppendEntry appends an entry to tail.
func (l *RaftLog) AppendEntry(entry Entry) {
	l.entries = append(l.entries, entry)
}

// AppendEntry appends the entries to tail.
func (l *RaftLog) AppendEntries(entries []Entry) {
	l.entries = append(l.entries, entries...)
}

// EntryAt returns the entry corresponding to the index of entry.
func (l *RaftLog) EntryAt(logIndex int) Entry {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	return l.entries[sliceIndex]
}

// EntriesAfter returns all the entries whose index is after the given index of entry.
func (l *RaftLog) EntriesAfter(logIndex int) []Entry {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	return l.entries[sliceIndex+1:]
}

// Update updates the entry at the given index of entry.
func (l *RaftLog) Update(logIndex int, entry Entry) {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	l.entries[sliceIndex] = entry
}

// RemoveAfter removes entries whose index is after the given index of entry.
func (l *RaftLog) RemoveAfter(logIndex int) {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	l.entries = l.entries[:sliceIndex+1]
}

// CommitTo advances committed index.
func (l *RaftLog) CommitTo(committed int) {
	if committed < l.committed {
		l.logger.Panicf("RaftLog: new committed(%d) is less than current committed(%d)", committed, l.committed)
	}
	l.committed = committed
}

// ApplyTo advances applied index.
func (l *RaftLog) ApplyTo(applied int) {
	if applied < l.applied {
		l.logger.Panicf("RaftLog: new applied(%d) is less than current applied(%d)", applied, l.applied)
	}
	l.applied = applied
}

// ToEntryIndex returns the index of entry corresponding to the slice index.
func (l *RaftLog) ToEntryIndex(sliceIndex int) int {
	return l.entries[0].Index + sliceIndex
}

// ToSliceIndex return the slice index in corresponding to the index of entry.
func (l *RaftLog) ToSliceIndex(logIndex int) int {
	return logIndex - l.entries[0].Index
}

// Compact removes the entries that have been compacted into snapshot.
func (l *RaftLog) Compact(index int) {

}

// validateIndex validates the slice index.
func (l *RaftLog) validateIndex(sliceIndex int) {
	entLen := len(l.entries)
	if sliceIndex < 0 || sliceIndex >= entLen {
		l.logger.Panicf("RaftLog: index %s is out bound of [0, %d]", sliceIndex, entLen)
	}
}
