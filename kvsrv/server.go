package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LastOperation struct {
	RPCID int64
	OldValue string
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]string
	Log map[int64]LastOperation
}

func (kv *KVServer) LogCheck(clientID int64, rpcID int64, oldValue *string) bool {
	lastOperation, exist := kv.Log[clientID]
	if !exist {				// 如果cliend_id不存在,返回true
		return true
	}
	if lastOperation.RPCID == rpcID {	// 如果cliend_id存在,且rpc_id和最近一次的rpc_id相同,则返回false
		(*oldValue) = lastOperation.OldValue
		return false
	} else {				// 否则,删掉cliend_id的rpc_id,并返回true
		delete(kv.Log, clientID)
		return true
	}
}

func (kv *KVServer) ReWriteLog() { 
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exist := kv.data[args.Key]
	if !exist {
		reply.Value = ""
	} else {
		reply.Value = value
	}
}	

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldValue := new(string)
	if !kv.LogCheck(args.ClientID, args.RPCID, oldValue) {
		return
	}
	
	kv.data[args.Key] = args.Value

	kv.Log[args.ClientID] = LastOperation{RPCID: args.RPCID, OldValue: ""}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldValue := new(string)
	if !kv.LogCheck(args.ClientID, args.RPCID, oldValue) {
		reply.Value = *oldValue
		return
	}

	oldvalue := kv.data[args.Key]
	kv.data[args.Key] = oldvalue + args.Value
	reply.Value = oldvalue

	kv.Log[args.ClientID] = LastOperation{RPCID: args.RPCID, OldValue: reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.Log = make(map[int64]LastOperation)

	return kv
}
