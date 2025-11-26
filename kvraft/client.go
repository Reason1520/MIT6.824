package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "fmt"

const PRINTINF = false


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.

// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)

// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		ClientID: ck.clerkID,
		RPCID: nrand(),
	}
	if PRINTINF{fmt.Printf("try get :%s\n", args.Key)}

	// 循环遍历server列表发送信息
	ok := false
	i := 0
	for {
		reply := GetReply{}
		serverIndex := i % len(ck.servers)
		ok = ck.servers[serverIndex].Call("KVServer.Get", &args, &reply)
		if ok {
			// 当RPC调用成功时，判断错误信息
			if reply.Err == OK || reply.Err == ErrRepeated {
				// 如果get成功，则返回结果
				if PRINTINF{fmt.Printf("Get %s :%s\n", args.Key, reply.Value)}
				return reply.Value
			} else if reply.Err == ErrNoKey {
				// 如果没有该key，则返回空字符串
				return ""
			} else if reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
				// 如果超时或不是leader，继续尝试下一个
			}
		}
		i++
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		ClientID: ck.clerkID,
		RPCID: nrand(),
	}

	if PRINTINF{fmt.Printf("try %s :%s %s\n", op, args.Key, args.Value)}

	// 循环遍历server列表发送信息
	ok := false
	i := 0
	for {
		reply := PutAppendReply{}
		serverIndex := i % len(ck.servers)
		ok = ck.servers[serverIndex].Call("KVServer."+ op, &args, &reply)
		if ok {
			// 当RPC调用成功时，判断错误信息
			if reply.Err == OK || reply.Err == ErrRepeated {
				// 如果put/append成功或者重复，则退出循环
				break
			} else if reply.Err == ErrNoKey{
				// 如果key不存在，则return
				return
			} else if reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
				// 如果超时或不是leader，继续尝试下一个
			}
		}
		i++
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
