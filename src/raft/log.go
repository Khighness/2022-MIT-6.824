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
	entries []Entry // current log entries

	committed int // committed index
	applied   int // applied index

	lastSnapshotTerm  int    // the term of the last entry in snapshot
	lastSnapshotIndex int    // the index of the last entry in snapshot
	snapshot          []byte // snapshot data

	logger *zap.SugaredLogger
}

// NewRaftLog creates a RaftLog instance.
func NewRaftLog(entries []Entry, committed, applied int,
	lastSnapshotTerm, lastSnapshotIndex int, snapshot []byte) *RaftLog {
	raftLog := &RaftLog{
		entries:           entries,
		committed:         committed,
		applied:           applied,
		lastSnapshotTerm:  lastSnapshotTerm,
		lastSnapshotIndex: lastSnapshotIndex,
		snapshot:          snapshot,
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

//  NewRaftLog creates a RaftLog instance by labgob.LabDecoder.
func NewRaftLogFromDecoder(statDecoder *labgob.LabDecoder, snapDecoder *labgob.LabDecoder) *RaftLog {
	var (
		entries           []Entry
		committed         int
		applied           int
		lastSnapshotIndex int
		lastSnapshotTerm  int
		snapshot          []byte
	)

	if statDecoder.Decode(&entries) != nil ||
		statDecoder.Decode(&committed) != nil ||
		statDecoder.Decode(&applied) != nil ||
		statDecoder.Decode(&lastSnapshotIndex) != nil ||
		statDecoder.Decode(&lastSnapshotTerm) != nil {
		panic("failed to decode raft log's state")
	}

	if snapDecoder != nil && snapDecoder.Decode(&snapshot) != nil {
		panic("failed to decode raft log's snapshot")
	}

	return NewRaftLog(entries, committed, applied, lastSnapshotTerm, lastSnapshotIndex, snapshot)
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

// Compact compacts the entries.
func (l *RaftLog) Compact(index int, snapshot []byte) {
	if index < l.FirstIndex() || index > l.LastIndex() {
		l.logger.Warnf("Compact: index(%d) is not in [%d, %d]", index, l.FirstIndex(), l.LastIndex())
	}

	l.entries = l.entries[index-l.lastSnapshotIndex:]
	l.lastSnapshotIndex = index
	l.lastSnapshotTerm = l.EntryAt(index).Term
	l.snapshot = snapshot
}

// ApplySnapshot applies the snapshot.
func (l *RaftLog) Apply(index int, term int, snapshot []byte) {
	if index < l.FirstIndex() {
		l.logger.Warnf("Apply: index(%d) < lastSnapshotIndex(%d)", index, l.FirstIndex())
	}

	if index > l.LastIndex() {
		l = NewRaftLog(nil, index, index, term, index, snapshot)
	} else {
		l.Compact(index, snapshot)
	}
}

// EncodeState encodes RaftLog's state by labgob.LabEncoder.
func (l *RaftLog) EncodeState(encoder *labgob.LabEncoder) error {
	if err := encoder.Encode(l.entries); err != nil {
		return err
	}
	if err := encoder.Encode(l.committed); err != nil {
		return err
	}
	if err := encoder.Encode(l.applied); err != nil {
		return err
	}
	if err := encoder.Encode(l.lastSnapshotIndex); err != nil {
		return err
	}
	if err := encoder.Encode(l.lastSnapshotTerm); err != nil {
		return err
	}
	if err := encoder.Encode(l.snapshot); err != nil {
		return err
	}
	return nil
}

// EncodeSnapshot encodes RaftLog's snapshot by labgob.LabEncoder.
func (l *RaftLog) EncodeSnapshot(encoder *labgob.LabEncoder) error {
	return encoder.Encode(l.snapshot)
}

// validateIndex validates the slice index.
func (l *RaftLog) validateIndex(sliceIndex int) {
	entLen := len(l.entries)
	if sliceIndex < 0 || sliceIndex >= entLen {
		l.logger.Panicf("RaftLog: index %d is out bound of [0, %d)", sliceIndex, entLen)
	}
}
