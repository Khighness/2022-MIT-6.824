package raft

// @Author KHighness
// @Update 2023-04-08

// InstallSnapshotArgs structure.
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LastIncludedIndex int    // the index of the last entry in snapshot
	LastIncludedTerm  int    // the term of the last entry in snapshot
	Data              []byte // snapshot's data
}

// InstallSnapshotReply structure.
type InstallSnapshotReply struct {
	Term int // follower's term
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Snapshot is a proactive action of the upper level (eg. kv server).
// Just remove the entries that have already been compacted into snapshot
// and save the given snapshot data.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

// sendInstallSnapshotToPeer sends InstallSnapshotArgs to the specified peer.
func (rf *Raft) sendInstallSnapshotToPeer(peer int) {
}

// shouldSendInstallSnapshot checks if leader need to send InstallSnapshotArgs to the given peer.
func (rf *Raft) shouldSendInstallSnapshot(peer int) bool {
	prevLogIndex := rf.progress[peer].Next - 1
	return prevLogIndex < rf.raftLog.FirstIndex()
}

// InstallSnapshot handles InstallSnapshotArgs and replies InstallSnapshotReply.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

}

// SendInstallSnapshot calls InstallSnapshot RPC.
func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
