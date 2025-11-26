package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRepeated   = "ErrRepeated"
	ErrTimeout = "ErrTimeout"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// PutAppendArgs : Put or Append RPC request arguments structure.
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	RPCID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	RPCID int64
}

type GetReply struct {
	Err   Err
	Value string
}
