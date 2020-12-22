package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(fmt.Sprintf("%d, ", time.Now().UnixNano())+format+"\n", a...)
	}
	return
}
