package shardkv 

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Type string
	Key string
	Value string

	SeqNum int64
	Client string
	UID int64

	Config shardmaster.Config //proposed new configration for Reconfig
	Reconfig GetShardReply

}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	content map[string] string
	seen map[string] int64
	replies map[string] string

	processed int
	config shardmaster.Config
}


func (kv *ShardKV)WaitAgreement(seq int) Op{
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}


func (kv *ShardKV) CheckOp(op Op) (string, string) {

	switch op.Type{
	case "Reconfigure" :
		if kv.config.Num >= op.Config.Num {
			return "", OK
		}

	case "Get","Put","Append" :
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard] {
			return "", ErrWrongGroup
		}

		seqnum, exists := kv.seen[op.Client]
		if exists && op.SeqNum <= seqnum {
			return kv.replies[op.Client], OK
		}
	}

	// fmt.Println("CheckOp default")

	return "",""
}

func (kv *ShardKV) ApplyGet(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum
}

func (kv *ShardKV) ApplyPut(op Op) {
	// fmt.Println("ApplyPut key:",op.Key," val:",op.Value)
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum

	kv.content[op.Key] = op.Value
}


func (kv *ShardKV) ApplyAppend(op Op) {
	fmt.Println("ApplyAppend key:",op.Key," val:",op.Value)
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum

	kv.content[op.Key] = kv.content[op.Key] + op.Value
}

func (kv *ShardKV) ApplyReconfigure(op Op) {
	info := &op.Reconfig
	for key := range info.Content {
		kv.content[key] = info.Content[key]
	}
	for client := range info.Seen {
		seqnum, exists := kv.seen[client]
		if !exists || seqnum < info.Seen[client] {
			kv.seen[client] = info.Seen[client]
			kv.replies[client] = info.Replies[client]
		}
	}
	kv.config = op.Config
}

func (kv *ShardKV) Apply(op Op) {
	switch op.Type{
	case "Get":
		kv.ApplyGet(op)
	case "Put":
		kv.ApplyPut(op)
	case "Append":
		kv.ApplyAppend(op)
	case "Reconfigure":
		kv.ApplyReconfigure(op)
	case "GetShard":

	}

	kv.processed++
	kv.px.Done(kv.processed)
}


func (kv *ShardKV) AddOp(op Op) (string, string) {
	var ok = false
	op.UID = nrand()
	for !ok {
		res, ret := kv.CheckOp(op)
		if ret != "" {
			return res, ret
		}
		seq := kv.processed + 1
		decide, t := kv.px.Status(seq)
		var opFinal Op
		if decide {
			opFinal = t.(Op)
		} else {
			// fmt.Println("AddOp start")
			kv.px.Start(seq, op)
			// fmt.Println("AddOp WaitAgreement")
			opFinal = kv.WaitAgreement(seq)
		}
		// fmt.Println("op UID:",op.UID," opFinal.UID: ",opFinal.UID)
		if op.UID == opFinal.UID {
			// fmt.Println("AddOp equal")
			ok = true
		}
		kv.Apply(opFinal)
	}
	// fmt.Println("AddOp return  replies:", kv.replies[op.Client])
	return kv.replies[op.Client], OK
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, seqnum, ck := args.Key, args.SeqNum, args.Me
	fmt.Println("PutAppend key:",key," client:",ck)
	reply.Value, reply.Err = kv.AddOp(Op{Type:"Get", Key:key, SeqNum:seqnum, Client:ck})
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, value, seqnum, ck, op := args.Key, args.Value, args.SeqNum, args.Me, args.Op
	fmt.Println("PutAppend key:",key," val:",value," client:",ck," seqnum:",seqnum)
	_, reply.Err = kv.AddOp(Op{Type:op, Key:key, Value:value, SeqNum:seqnum, Client:ck })
	return nil
}


func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.AddOp(Op{Type:"GetShard"})

	reply.Content = map[string]string{}
	reply.Seen = map[string]int64{}
	reply.Replies = map[string]string{}

	for key := range kv.content {
		if key2shard(key) == shard {
			reply.Content[key] = kv.content[key]
		}
	}

	for client := range kv.seen {
		reply.Seen[client] = kv.seen[client]
		reply.Replies[client] = kv.replies[client]
	}

	return nil
}


func (reply *GetShardReply) Merge(other GetShardReply) {
	for key := range other.Content {
		reply.Content[key] = other.Content[key]
	}
	for client := range other.Seen {
		seqnum, exists := reply.Seen[client]
		if !exists || seqnum < other.Seen[client] {
			reply.Seen[client] = other.Seen[client]
			reply.Replies[client] = other.Replies[client]
		}
	}
}


func (kv *ShardKV) Reconfigure(newConfig shardmaster.Config) bool {

	reConfig := GetShardReply{OK, map[string]string{}, map[string]int64{},
		map[string]string{}}

	oldConfig := &kv.config 

	for i := 0 ; i < shardmaster.NShards ; i++ {
		gid := oldConfig.Shards[i]
		if newConfig.Shards[i] == kv.gid && gid != kv.gid {
			args := &GetShardArgs{i, *oldConfig}
			var reply GetShardReply
			for _, server := range oldConfig.Groups[gid] {
				ok := call(server,"ShardKV.GetShard", args, &reply)
				if ok && reply.Err == OK {
					break;
				}
				if ok && reply.Err == ErrNotReady {
					return false
				}
			}

			reConfig.Merge(reply)
		}
	}

	op := Op{Type:"Reconfigure", Config:newConfig, Reconfig:reConfig}
	kv.AddOp(op)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := kv.sm.Query(-1)
	for i := kv.config.Num+1 ; i <= newConfig.Num ; i++ {
		config := kv.sm.Query(i)
		if !kv.Reconfigure(config) {
			return
		}
	}



}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)


	// Your initialization code here.
	kv.content = map[string] string{}
	kv.seen = map[string] int64{}
	kv.replies = map[string] string{}
	kv.processed = 0
	kv.config = shardmaster.Config{Num:-1}

	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
