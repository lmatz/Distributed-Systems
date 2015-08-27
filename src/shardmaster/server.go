package shardmaster 

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const (
	Join = 0
	Leave = 1
	Move = 2
	Query = 3
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	processed int
	currentConfigNum int
}


type Op struct {
	// Your data here.
	Type int
	GID int64
	Servers []string
	Shard int
	UID int64
	QueryNum int
}

func GetGidCounts(config *Config) (int64, int64) {
	min_id, min_num, max_id, max_num := int64(0),999,int64(0),-1

	counts := map[int64] int{}

	for g := range config.Groups {
		counts[g] = 0
	}

	for _, g := range config.Shards {
		counts[g] ++ 
	}

	for g := range counts {
		_, exists := config.Groups[g] 
		if exists && min_num > counts[g] {
			min_id, min_num = g, counts[g]
		}
		if exists && max_num < counts[g]{
			max_id, max_num = g, counts[g]
		}
	}
	for _, g := range config.Shards{
		if g == 0{
			max_id = 0
		}
	}
	return min_id, max_id
}

func GetShardByGid(gid int64, config *Config) int {
	for s, g := range config.Shards{
		if g == gid {
			return s
		}
	}
	return -1
}

func (sm *ShardMaster) Rebalance(gid int64, isLeave bool) {
	currentConfig := &sm.configs[sm.currentConfigNum]

	for i := 0 ; ; i++ {
		min_id, max_id := GetGidCounts(currentConfig)
		if isLeave {
			s := GetShardByGid(gid, currentConfig)
			if s == -1 {
				break
			}
			currentConfig.Shards[s] = min_id
		} else {
			if i == NShards / len(currentConfig.Groups) {
				break
			}
			s := GetShardByGid(max_id, currentConfig)
			currentConfig.Shards[s] = gid
			// fmt.Println("Rebalance false s: ",s," gid: ",gid)
		}
	}

}

func (sm *ShardMaster) NextConfig() *Config {
	oldConfig := &sm.configs[sm.currentConfigNum]
	var newConfig Config
	newConfig.Num = oldConfig.Num  + 1
	newConfig.Groups = map[int64] []string{}
	newConfig.Shards = [NShards]int64{}

	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for i, v := range oldConfig.Shards {
		newConfig.Shards[i] = v
	}

	sm.currentConfigNum ++
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.currentConfigNum]
}


func (sm *ShardMaster) ApplyJoin(gid int64, servers []string) {

	newConfig := sm.NextConfig()

	_, exists := newConfig.Groups[gid]

	if !exists {
		newConfig.Groups[gid] = servers
		sm.Rebalance(gid, false)
	}

}

func (sm *ShardMaster) ApplyLeave(gid int64) {

	newConfig := sm.NextConfig()

	_, exists := newConfig.Groups[gid]

	if exists {
		delete(newConfig.Groups, gid)
		sm.Rebalance(gid, true)
	}
}

func (sm *ShardMaster) ApplyMove(gid int64, shard int) {
	newConfig := sm.NextConfig()
	newConfig.Shards[shard] = gid
}

func (sm *ShardMaster) ApplyQuery(num int) Config {

	if num == -1 {
		// fmt.Println("ApplyQuery current: ",sm.currentConfigNum)
		return sm.configs[sm.currentConfigNum]
	} else {
		return sm.configs[num]
	}

}


func (sm *ShardMaster) Apply(seq int, op Op) Config {
	sm.processed++
	gid, servers, shard, num := op.GID, op.Servers, op.Shard, op.QueryNum
	
	switch op.Type {
		case Join:
			sm.ApplyJoin(gid, servers)
		case Leave:
			sm.ApplyLeave(gid)
		case Move:
			sm.ApplyMove(gid, shard)
		case Query:
			return sm.ApplyQuery(num)
		default:
			fmt.Println("Unexpected opeation type for ShardMaster!")
	}

	sm.px.Done(sm.processed)
	return Config{}
}

func (sm *ShardMaster) WaitAgreement(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := sm.px.Status(seq)
		if decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}


func (sm *ShardMaster) AddOp(op Op) Config {
	// fmt.Println("AddOp current: ",sm.currentConfigNum)
	op.UID = nrand()
	for {

		seq := sm.processed + 1

		decide, t := sm.px.Status(seq)
		// fmt.Printf("after Status\n")
		var opFinal Op

		if decide {
			opFinal = t.(Op)
		} else {
			sm.px.Start(seq,op)
			// fmt.Printf("after Start")
			opFinal = sm.WaitAgreement(seq)
			// fmt.Printf("after waitAgreement")
		}

		config := sm.Apply(seq,opFinal)
		// fmt.Printf("after apply")
		// fmt.Println("%d   %d",op.UID,opFinal.UID)
		if op.UID == opFinal.UID {
			return config
		}
	}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	gid , servers := args.GID, args.Servers

	sm.AddOp(Op{Type:Join, GID:gid, Servers:servers})


	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	gid := args.GID

	sm.AddOp(Op{Type:Leave, GID:gid})

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	shard, gid := args.Shard, args.GID

	sm.AddOp(Op{Type:Move, GID:gid, Shard:shard})

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	configNum := args.Num
	// fmt.Printf("%d Query is called and current ConfigNum is %d\n",sm.me,sm.currentConfigNum)
	config := sm.AddOp(Op{Type:Query, QueryNum:configNum})
	reply.Config = config
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.processed=0
	sm.currentConfigNum=0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
