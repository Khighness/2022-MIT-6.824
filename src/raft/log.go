package raft

import (
	"fmt"

	"6.824/labgob"
	"6.824/log"

	"go.uber.org/zap"
)

// @Author KHighness
// @Update 2023-04-08

// Entry structure.
type Entry struct {
	Term  int         // the term of log entry
	Index int         // the index of log entry
	Data  interface{} // the data of log entry
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
	// entries is a persistent state.
	// entries[0] is a dummy entry which stores the meta data of last snapshot.
	// entries[1:] are the entries that has not been compacted into the snapshot.
	entries []Entry // current log entries

	// NOTE: committed index and applied index are volatile state.
	// committed must be persisted in stable storage.
	committed int // committed index
	// applied should not be persisted in stable storage.
	// Because if there is snapshot in unit test (2D),
	// the call chain of Raft initialization is: config.start1() -> config.ingestSnap().
	// So config will use RaftLog.lastIncludedIndex as applied index instead of RaftLog.applied.
	applied int // applied index

	// Redundant storage: meta data of last snapshot.
	// lastIncludedTerm is the index of the last entry included in snapshot.
	lastIncludedTerm int // entries[0].Term
	// lastIncludedIndex is the term of the last entry included in snapshot.
	lastIncludedIndex int // entries[0].Index

	// NOTE: RaftLog should not stores the snapshot data.
	// Because there is limit for the log size: MAXLOGSIZE.

	logger *zap.SugaredLogger
}

// NewRaftLog creates a RaftLog instance.
func NewRaftLog(entries []Entry, committed, applied int, lastSnapshotTerm, lastSnapshotIndex int) *RaftLog {
	raftLog := &RaftLog{
		entries:           entries,
		committed:         committed,
		applied:           applied,
		lastIncludedTerm:  lastSnapshotTerm,
		lastIncludedIndex: lastSnapshotIndex,
		logger:            log.NewZapLogger("RaftLog", zap.WarnLevel).Sugar(),
	}

	if len(entries) == 0 { // Create a dummy entry.
		raftLog.entries = []Entry{{
			Term:  lastSnapshotTerm,
			Index: lastSnapshotIndex,
		}}
	}

	return raftLog
}

//  NewRaftLog creates a RaftLog instance by labgob.LabDecoder.
func NewRaftLogFromDecoder(statDecoder *labgob.LabDecoder) *RaftLog {
	var (
		entries           []Entry
		committed         int
		lastIncludedIndex int
		lastIncludedTerm  int
	)

	if statDecoder.Decode(&entries) != nil ||
		statDecoder.Decode(&committed) != nil ||
		statDecoder.Decode(&lastIncludedIndex) != nil ||
		statDecoder.Decode(&lastIncludedTerm) != nil {
		panic("failed to decode raft log's state")
	}

	return NewRaftLog(entries, committed, lastIncludedIndex, lastIncludedTerm, lastIncludedIndex)
}

// String returns RaftLog's string.
func (l *RaftLog) String() string {
	return fmt.Sprintf("[first = %d, applied = %d, committed = %d, last = %d]",
		l.FirstIndex(), l.applied, l.committed, l.LastIndex())
}

// FirstIndex returns the index of the first entry.
// It is 0 if there is no snapshot.
// Otherwise, it is the index of last snapshot.
func (l *RaftLog) FirstIndex() int {
	return l.entries[0].Index
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

// Get returns the entry corresponding to the slice index.
func (l *RaftLog) Get(sliceIndex int) Entry {
	return l.entries[sliceIndex]
}

// EntryAt returns the entry corresponding to the index of entry.
func (l *RaftLog) EntryAt(logIndex int) Entry {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	return l.entries[sliceIndex]
}

// EntriesAfter returns the entries whose index is after the given index of entry.
func (l *RaftLog) EntriesAfter(logIndex int) []Entry {
	sliceIndex := l.ToSliceIndex(logIndex)
	l.validateIndex(sliceIndex)
	entries := make([]Entry, l.LastIndex()-logIndex)
	copy(entries, l.entries[sliceIndex+1:])
	return entries
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

// CompactTo compacts the entries.
// Just removes the entries whose index is less than the given index.
func (l *RaftLog) CompactTo(index int) {
	if index <= l.FirstIndex() || index > l.LastIndex() {
		l.logger.Warnf("CompactTo: index(%d) is out bound of (%d, %d]", index, l.FirstIndex(), l.LastIndex())
		return
	}

	l.lastIncludedTerm = l.EntryAt(index).Term
	l.committed = max(l.committed, index)
	l.applied = max(l.applied, index)
	l.entries = l.entries[index-l.lastIncludedIndex+1:]
	l.lastIncludedIndex = index

	newEntries := make([]Entry, 1)
	newEntries[0] = Entry{Term: l.lastIncludedTerm, Index: l.lastIncludedIndex}
	l.entries = append(newEntries, l.entries...)
}

// ApplySnapshot applies the snapshot and maybe return a new instance.
func (l *RaftLog) Apply(index int, term int) *RaftLog {
	if index <= l.FirstIndex() {
		l.logger.Warnf("Apply: index(%d) <= lastIncludedIndex(%d)", index, l.FirstIndex())
		return l
	}

	if index > l.LastIndex() {
		l = NewRaftLog(nil, index, index, term, index)
	} else {
		l.CompactTo(index)
		l.committed = index
		l.applied = index
	}

	return l
}

// Encode encodes RaftLog's state by labgob.LabEncoder.
func (l *RaftLog) Encode(encoder *labgob.LabEncoder) error {
	var err error
	if err = encoder.Encode(l.entries); err != nil {
		return err
	}
	if err = encoder.Encode(l.committed); err != nil {
		return err
	}
	if err = encoder.Encode(l.lastIncludedIndex); err != nil {
		return err
	}
	if err = encoder.Encode(l.lastIncludedTerm); err != nil {
		return err
	}
	return nil
}

// validateIndex validates the slice index.
func (l *RaftLog) validateIndex(sliceIndex int) {
	entLen := len(l.entries)
	if sliceIndex < 0 || sliceIndex >= entLen {
		l.logger.Panicf("RaftLog: index %d is out bound of [0, %d)", sliceIndex, entLen)
	}
}
