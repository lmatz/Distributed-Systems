package shardkv 

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Me string
	SeqNum int64
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	Me string
	SeqNum int64
}

type GetReply struct {
	Err   string
	Value string
}

type GetShardArgs struct {
	Shard int
	Config shardmaster.Config //provider has to under at least this configuration
}

type GetShardReply struct {
	Err Err
	Content map[string]string
	Seen map[string]int64
	Replies map[string]string
}

