package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
	)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader *labrpc.ClientEnd
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = servers[0]
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	args := GetArgs{Key: key}

	call := func(server *labrpc.ClientEnd) GetReply {
		innerReply := GetReply{}
		done := make(chan bool)
		ok := false
		go func() {
			done <- server.Call("RaftKV.Get", &args, &innerReply)
		}()
		select {
		case ok = <-done:
		case <-time.After(360 * time.Second):
			DPrintf("client.Get timeout. ")
		}
		DPrintf("client.Call.Get finished. ok: %v, innerReply: %v", ok, innerReply)
		if ok {
			return innerReply
		} else {
			return GetReply{WrongLeader: true}
		}
	}
	if reply := call(ck.lastLeader); !reply.WrongLeader {
		value = reply.Value
	} else {
	outer:
		for {
			for _, server := range ck.servers {
				if reply := call(server); !reply.WrongLeader {
					value = reply.Value
					ck.lastLeader = server
					break outer
				}
			}
		}
	}
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	id := nrand()
	args := PutAppendArgs{Key: key, Value: value, Op: op, Id: id}

	call := func(server *labrpc.ClientEnd) PutAppendReply {
		innerReply := PutAppendReply{}
		done := make(chan bool)
		ok := false
		go func() {
			done <- server.Call("RaftKV.PutAppend", &args, &innerReply)
		}()
		select {
		case ok = <-done:
		case <-time.After(360 * time.Second):
			DPrintf("client.Call.PutAppend timeout. ")
		}
		DPrintf("client.Call.PutAppend finished. ok: %v, innerReply: %v. dupErr: %v", ok, innerReply, innerReply.Err == ErrDupReq)
		if ok {
			return innerReply
		} else {
			return PutAppendReply{WrongLeader: true}
		}
	}
	if leaderReply := call(ck.lastLeader); !leaderReply.WrongLeader || leaderReply.Err == ErrDupReq {
		return
	}
outer:
	for {
		for _, server := range ck.servers {
			if serverReply := call(server); !serverReply.WrongLeader || serverReply.Err == ErrDupReq {
				ck.lastLeader = server
				break outer
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
