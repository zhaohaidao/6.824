package raft

import (
	"log"
	"fmt"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(fmt.Sprintf("RaftLog: %s", format), a...)
	}
	return
}
