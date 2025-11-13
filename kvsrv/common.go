package kvsrv

// PutAppendArgs : Put or Append arguments.
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
	Value string
}

type GetArgs struct {
	Key string
	ClientID int64
	RPCID int64
}

type GetReply struct {
	Value string
}
