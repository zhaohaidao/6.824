package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	)

const Debug = 0

const (
	OpGet = "Get"
	OpPut = "Put"
	OpAppend = "Append"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 0 {
		log.Printf(fmt.Sprintf("ServerLog: %s", format), a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	Type string
	Key string
	Value string
	Id int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValues map[string]string

	applyWaitMap map[int]chan struct{}

	uniqIdMap map[int64]struct{}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	index, _, isLeader := kv.rf.Start(Op{Type: OpGet, Key: key})
	reply.WrongLeader = !isLeader
	DPrintf("server.Get: server: %v, isLeader: %v, reply: %v", kv.me, isLeader, reply)
	if reply.WrongLeader {
		return
	}
	kv.mu.Lock()
	waitCh := make(chan struct{})
	kv.applyWaitMap[index] = waitCh
	kv.mu.Unlock()
	ok := false
	select {
	case _, ok = <-waitCh:
	}
	if ok {
		kv.mu.Lock()
		if val, ok := kv.keyValues[key]; ok {
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	id := args.Id
	dup := func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if _, ok := kv.uniqIdMap[id]; ok {
			reply.Err = ErrDupReq
			return true
		} else {
			return false
		}
	}()
	if dup {
		return
	}
	op := args.Op
	key := args.Key
	value := args.Value
	index, _, isLeader := kv.rf.Start(Op{Type: op, Key: key, Value: value, Id: id})
	reply.WrongLeader = !isLeader
	DPrintf("server.PutAppend: server: %v, isLeader: %v, reply: %v", kv.me, isLeader, reply)
	if reply.WrongLeader {
		return
	}
	kv.mu.Lock()
	waitCh := make(chan struct{})
	kv.applyWaitMap[index] = waitCh
	kv.mu.Unlock()
	ok := false
	select {
	case _, ok = <-waitCh:
	}
	if !ok {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.keyValues = map[string]string{}
	kv.applyWaitMap = map[int]chan struct{}{}
	kv.uniqIdMap = map[int64]struct{}{}

	// You may need initialization code here.

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			select {
			case command := <- kv.applyCh:
				op := command.Command.(Op)
				kv.mu.Lock()
				if _, ok := kv.uniqIdMap[op.Id]; !ok {
					kv.uniqIdMap[op.Id] = struct{}{}
					if op.Type == OpPut {
						kv.keyValues[op.Key] = op.Value
					} else if op.Type == OpAppend {
						kv.keyValues[op.Key] += op.Value
						DPrintf("APPLYAPPEND: key: %v, server: %v, index: %v, value: %v", op.Key, kv.me, command.Index, kv.keyValues[op.Key])
					}
				}
				if waitCh, ok := kv.applyWaitMap[command.Index]; ok {
					waitCh <- struct{}{}
				}
				kv.mu.Unlock()
			case _ = <- kv.rf.StateChangeCh:
				kv.mu.Lock()
				for index, waitCh := range kv.applyWaitMap {
					close(waitCh)
					delete(kv.applyWaitMap, index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
