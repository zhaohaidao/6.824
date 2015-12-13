package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import (
	"sync/atomic"
	"errors"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	currentView View
	ackedViewNum uint
	visitRecord map[string]time.Time


}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	if args.Me != "" {
		vs.visitRecord[args.Me] = time.Now()
	}

	// Your code here.
	if args.Me == vs.currentView.Primary && vs.currentView.Viewnum == args.Viewnum {
		vs.ackedViewNum = vs.currentView.Viewnum
	}

	if vs.currentView.Viewnum == 0 {
		if args.Viewnum == vs.currentView.Viewnum {
			vs.ackedViewNum = 0
			vs.currentView.Viewnum += 1
			vs.currentView.Primary = args.Me
			vs.currentView.Backup = ""
			reply.View = vs.currentView
			return nil
		} else {
			return errors.New("abnormal VIEWNUM !!!")
		}
	}

	if vs.currentView.Backup == "" {
		if vs.currentView.Primary == args.Me {
			reply.View = vs.currentView
			return nil
		} else {
			vs.currentView.Backup = args.Me
			if vs.ackedViewNum == vs.currentView.Viewnum {
				vs.currentView.Viewnum += 1
			}
			reply.View = vs.currentView
			return nil
		}
	}

	if  args.Viewnum == 0 && vs.currentView.Primary == args.Me {
		if vs.currentView.Primary != "" && vs.currentView.Backup != "" {
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = ""
			if vs.currentView.Viewnum == vs.ackedViewNum {
				vs.currentView.Viewnum += 1
			}
		}

	}

	reply.View = vs.currentView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	currentView := vs.currentView
	if currentView.Viewnum > 0 && vs.ackedViewNum == currentView.Viewnum {
		if currentView.Primary != "" {
			lastVisitTime, ok := vs.visitRecord[currentView.Primary]
			if ok {
				if (time.Now().Sub(lastVisitTime)).Nanoseconds()/int64(PingInterval) > DeadPings {
					currentView.Primary = ""
				}
			}

		}

		if currentView.Backup != "" {
			lastVisitTime, ok := vs.visitRecord[currentView.Backup]
			if ok {
				if (time.Now().Sub(lastVisitTime)).Nanoseconds()/int64(PingInterval)  > DeadPings {
					currentView.Backup = ""
				} else if currentView.Primary == "" {
					currentView.Primary = currentView.Backup
					currentView.Backup = ""
				}
			}
		}
		if currentView.Primary != vs.currentView.Primary ||
		currentView.Backup != vs.currentView.Backup {
			currentView.Viewnum += 1
		}

	}

	vs.currentView = currentView
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{Viewnum:0, Primary:"", Backup:""}
	vs.visitRecord = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
