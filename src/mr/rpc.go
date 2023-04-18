package mr

import (
	"os"
	"strconv"
)

// RegisterArgs structure.
type RegisterArgs struct {
}

// RegisterReply structure.
type RegisterReply struct {
	WorkerId int
}

// ApplyTaskArgs structure.
type ApplyTaskArgs struct {
	WorkerId int
}

// ApplyTaskReply structure.
type ApplyTaskReply struct {
	Task *Task
}

// ReportTaskArgs structure.
type ReportTaskArgs struct {
	WorkerId int
	Phase    TaskPhase
	Done     bool
}

// ReportTaskReply structure.
type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
