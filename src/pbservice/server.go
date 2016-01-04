package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	randNumRecord map[int64]bool
	currentView viewservice.View
	kv         map[string]string
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.currentView.Primary == pb.me {
		k := args.Key
		v, ok := pb.kv[k]
		if ok {
			reply.Value = v
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}


func (pb *PBServer) forwardReq(args *PutAppendArgs) {
	args.ReqType = ForwardReq
	var reply PutAppendReply
	ok := call(pb.currentView.Backup, "PBServer.PutAppend", args, &reply)
	if !ok {
		log.Println(reply.Err)
	}

}

func (pb *PBServer) transfer(backup string) {

	log.Println("Transfer whole kv to " + backup)
	args := &TransferArgs{}
	args.Kv = pb.kv
	var reply TransferReply
	ok := call(backup, "PBServer.Transfer", args, &reply)
	if !ok {
		log.Println("transfer failed: " + reply.Err)

	}
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.kv = args.Kv
	reply.Err = OK
	return nil
}

func (pb *PBServer) put(key string, newVal string) {
	pb.kv[key] = newVal
}

func (pb *PBServer) append(key string, newVal string) {
	previousValue, ok := pb.kv[key]
	if ok {
		newValue := previousValue + newVal
		pb.kv[key] = newValue
	} else {
		pb.kv[key] = newVal
		log.Println(pb.kv[key])
	}

}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Your code here.
	handler := func() {
		k := args.Key
		v := args.Value
		randNum := args.RandNum
		_, ok := pb.randNumRecord[randNum]
		if ok {
			return
		} else {
			pb.randNumRecord[randNum] = true
			switch args.Op {
			case "Put":
				pb.put(k, v)
			case "Append":
				pb.append(k, v)
			}
			if pb.currentView.Primary == pb.me && pb.currentView.Backup != "" {
				pb.forwardReq(args)
			}

		}
	}
	reqType := args.ReqType
	if pb.currentView.Primary == pb.me {
		if reqType == DirectReq {
			handler()
			reply.Err = OK
		} else {
			reply.Err = ErrWrongServer
		}
	} else if pb.currentView.Backup == pb.me {
		if reqType == ForwardReq {
			handler()
			reply.Err = OK
		} else {
			reply.Err = ErrWrongServer
		}
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.

	fmt.Println(pb.me + " ticking to viewservice")
	pb.mu.Lock()
	defer pb.mu.Unlock()
	v, _ :=pb.vs.Ping(pb.currentView.Viewnum)
	if v.Viewnum != pb.currentView.Viewnum {
		fmt.Print(pb.currentView)
		fmt.Print("\tupdate to\t")
		fmt.Println(v)
		oldBackup := pb.currentView.Backup
		newBackup := v.Backup
		if pb.currentView.Primary == pb.me && newBackup != "" && oldBackup == "" {
			pb.transfer(newBackup)
		}
		pb.currentView = v
	}

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.randNumRecord = make(map[int64]bool)
	pb.kv = make(map[string]string)
	pb.currentView = viewservice.View{Viewnum:0, Primary:"", Backup:""}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
