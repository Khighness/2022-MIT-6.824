package raft

// @Author KHighness
// @Update 2023-04-10

// NodeRole represents the role of a node in a cluster.
type NodeRole uint8

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

var roleNameMap = [...]string{
	"Follower",
	"Candidate",
	"Leader",
}

var roleShortNameMap = [...]string{
	"F",
	"C",
	"L",
}

// String returns role's full name.
func (nr NodeRole) String() string {
	return roleNameMap[int(nr)]
}

// String returns role's short name.
func (nr NodeRole) ShortString() string {
	return roleShortNameMap[int(nr)]
}
