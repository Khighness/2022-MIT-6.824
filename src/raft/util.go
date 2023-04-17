package raft

import (
	"log"
	"math/rand"
	"runtime"
	"strings"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func randTimeDuration(lower, upper time.Duration) time.Duration {
	randTime := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(randTime) * time.Nanosecond
}

func getCalledFunction() string {
	caller, _, _, _ := runtime.Caller(2)
	action := runtime.FuncForPC(caller).Name()
	return action[strings.LastIndex(action, ".")+1:]
}

type intSlice []int

func (s intSlice) Len() int           { return len(s) }
func (s intSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s intSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
