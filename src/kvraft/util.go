package kvraft

import (
	"runtime"
	"strings"
)

// @Author KHighness
// @Update 2023-04-22

const enableLockLog = true

func getCalledFunction() string {
	caller, _, _, _ := runtime.Caller(2)
	action := runtime.FuncForPC(caller).Name()
	return action[strings.LastIndex(action, ".")+1:]
}
