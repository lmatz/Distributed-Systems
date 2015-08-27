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
import "errors"
// import "strconv"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	content map[string] string
	currentView 		viewservice.View
}

func (pb *PBServer) isPrimary() bool{
	return pb.currentView.Primary == pb.me
}

func (pb *PBServer) isBackup() bool{
	return pb.currentView.Backup == pb.me
}

func (pb *PBServer) hasPrimary() bool{
	return pb.currentView.Primary != ""
}

func (pb *PBServer) hasBackup() bool{
	return pb.currentView.Backup != ""
}

func (pb *PBServer) Forward(args *ForwardArgs) error {
	if !pb.hasBackup(){
		return nil
	}
	var reply ForwardReply
	ok := call(pb.currentView.Backup, "PBServer.ProcessForward", args, &reply)
	if !ok {
		return errors.New("[Foward] failed to forward put")
	}
	return nil
}

func (pb *PBServer) ProcessForward(args *ForwardArgs, reply *ForwardReply) error{
	pb.mu.Lock()

	defer pb.mu.Unlock()

	if !pb.isBackup(){
		return errors.New("I'm not backup")
	}

	// if op == "Put" {
		for key,value := range args.Content{
			pb.content[key] = value
		}
	// } else {

	// }

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()

	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return errors.New("[get] i'm not primary, received Get request")
	}
	reply.Value = pb.content[args.Key]

	

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()

	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return errors.New("I am not the primary server")
	}

	key, value := args.Key, args.Value

	op := args.Op

	client, uid := args.Me, args.UUID

	if pb.content["seen."+client] == uid {
		reply.PreviousValue = pb.content["oldreply." + client]
		return nil
	}

	if op == "Put" {

	} else if op == "Append" {
		value = pb.content[key]+value
	}

	forwardArgs := &ForwardArgs{map[string]string{
		key:value,
		"seen."+client : uid,
		"oldreply."+client : reply.PreviousValue,
		}}

	err := pb.Forward(forwardArgs)

	if err != nil {
		return errors.New("forward fail")
	}

	for keyT, valueT := range forwardArgs.Content{
		pb.content[keyT] = valueT
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
	pb.mu.Lock()

	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.currentView.Viewnum)
	if err!=nil {

	}

	needForward := view.Backup !="" && view.Backup!=pb.currentView.Backup && pb.isPrimary()

	pb.currentView = view

	if needForward {
		// fmt.Println("process forward in tick")
		// fmt.Println(pb.content)
		pb.Forward(&ForwardArgs{Content:pb.content})
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
	pb.content = map[string]string{}

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
