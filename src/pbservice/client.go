package pbservice

import "viewservice"
import "net/rpc"

import "crypto/rand"
import (
	"math/big"
	"time"
)


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	isPDead bool
	primary string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.primary = ck.vs.Primary()
	ck.isPDead = true

	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//

func (ck *Clerk) Get(key string) string {

	// Your code here.
	for ck.isPDead {
		primary := ck.vs.Primary()
		if primary != "" {
			ck.isPDead = false
			ck.primary = primary
		}
	}
	value := ""

	if !ck.isPDead {
		args := &GetArgs{}
		args.Key = key
		var reply GetReply
		ok := call(ck.primary, "PBServer.Get", args, &reply)
		if ok {
			if reply.Err == OK {
				value =  reply.Value
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				ck.isPDead = true
				ck.primary = ""
				time.Sleep(viewservice.PingInterval)
				return ck.Get(key)
			}
		} else {
			ck.isPDead = true
			ck.primary = ""
			time.Sleep(viewservice.PingInterval)
			return ck.Get(key)
		}

	}
	return value

}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string, randNum int64) {

	// Your code here.
	// prepare the arguments.
	for ck.isPDead {

		primary := ck.vs.Primary()
		if primary != "" {
			ck.isPDead = false
			ck.primary = primary
		}
	}

	if !ck.isPDead {
		args := &PutAppendArgs{}
		args.Key = key
		args.Value = value
		args.Op = op
		args.ReqType = DirectReq
		args.RandNum = randNum
		var reply PutAppendReply
		ok := call(ck.primary, "PBServer.PutAppend", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrWrongServer {
				ck.isPDead = true
				ck.primary = ""
				time.Sleep(viewservice.PingInterval)
				ck.PutAppend(key, value, op, randNum)
			}
		} else {
			ck.isPDead = true
			ck.primary = ""
			time.Sleep(viewservice.PingInterval)
			ck.PutAppend(key, value, op, randNum)
		}

	}

}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put", nrand())
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append", nrand())
}
