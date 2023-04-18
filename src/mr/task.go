package mr

import "time"

// @Author KHighness
// @Update 2023-04-18

// TaskPhase represents the stage of Task.
type TaskPhase uint8

const (
	TaskPhaseMap TaskPhase = iota
	TaskPhaseReduce
)

// TaskStatus represents the status of Task.
type TaskStatus uint8

const (
	TaskStatusInitial TaskStatus = iota
	TaskStatusRunning
	TaskStatusFinished
)

// Task structure.
type Task struct {
	filename  string
	content   string
	status    TaskStatus
	createdAt time.Time
}

// TaskState structure.
type TaskState struct {
	status    TaskStatus
	workerId  int
	startTime time.Time
}
