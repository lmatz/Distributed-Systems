package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	currentView View
	primaryAck uint
	primaryTick uint
	backupAck uint
	backupTick uint
	currentTick uint
}

func (vs *ViewServer) Acked() bool{
	return vs.currentView.Viewnum == vs.primaryAck
}

func (vs *ViewServer) Uninitialized() bool {
	return vs.currentView.Viewnum == 0 && vs.currentView.Primary == ""
}

func (vs *ViewServer) HasPrimary() bool {
	return vs.currentView.Primary != ""
}

func (vs *ViewServer) HasBackup() bool{
	return vs.currentView.Backup != ""
}

func (vs *ViewServer) IsPrimary(name string) bool {
	return vs.currentView.Primary == name
}

func (vs *ViewServer) IsBackup(name string) bool {
	return vs.currentView.Backup == name
}

func (vs *ViewServer) PromoteBackup() {
	if !vs.HasBackup() {

		return
	}
	vs.currentView.Primary = vs.currentView.Backup
	vs.currentView.Backup = ""
	vs.currentView.Viewnum++
	vs.primaryAck = vs.backupAck
	vs.primaryTick = vs.backupTick
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	name := args.Me
	num := args.Viewnum

	vs.mu.Lock()

	switch {

	case !vs.HasPrimary() && vs.currentView.Viewnum == 0 :
		vs.currentView.Primary = name
		vs.currentView.Viewnum = num+1
		vs.primaryTick = vs.currentTick
		vs.primaryAck = 0
	case vs.IsPrimary(name):
		if num == 0 {
			break;
		}
		vs.primaryAck = num
		vs.primaryTick = vs.currentTick
	case !vs.HasBackup() && vs.Acked() :
		vs.currentView.Backup = name
		vs.currentView.Viewnum++
		vs.backupTick = vs.currentTick
	case vs.IsBackup(name):
		if num == 0 && vs.Acked() {
			vs.currentView.Viewnum++
			vs.backupTick = vs.currentTick
		} else if num != 0 {
			vs.backupTick = vs.currentTick
		}
	}
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	vs.currentTick++
	if vs.currentTick - vs.primaryTick >= DeadPings && vs.Acked() {
		vs.PromoteBackup()
	}

	if vs.HasBackup() && vs.currentTick - vs.backupTick >= DeadPings && vs.Acked() {
		vs.currentView.Backup = ""
		vs.currentView.Viewnum++
	}
	vs.mu.Unlock()
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
	vs.currentView = View{0,"",""}
	vs.dead = 0
	vs.rpccount = 0
	vs.primaryAck = 0
	vs.primaryTick = 0
	vs.backupTick = 0
	vs.backupAck = 0


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
