package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key  string
	Value string
	ClientID int64	// 客户端ID标识
	RPCID int64		// 操作唯一标识
}

// OpResult : 操作结果
type OpResult struct {
	Err   Err
	Value string
	// 增加身份标识，用于校验是否对应的请求
	ClientID int64
	RPCID int64
	Term int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	persister *raft.Persister	// 持久化器,与raft的一致

	maxraftstate int // snapshot if log grows this big
	LastAppliedIndex int // 最后应用到状态机的log的索引

	// Your definitions here.
	Data map[string]string				// 存储键值对
	LastOp map[int64]OpResult			// 记录每个客户端最后一次操作
	notifyChans map[int]chan OpResult	// 用于通知客户端操作结果
	
}

// 封装统一的日志提交和等待逻辑
func (kv *KVServer) waitForApplied(op Op) OpResult {
	// 提交到 Raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 如果当前节点不是leader
		return OpResult{Err: ErrWrongLeader}
	}

	// 创建接收通道
	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.notifyChans[index] = ch
	kv.mu.Unlock()

	// 确保退出时清理 channel
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()

	// 等待结果
	select {
	case res := <-ch:
		// 校验当前 index 执行的命令是否是当前 RPC 提交的命令
		// 如果 Term 变了，或者 ClientID/RPCID 不匹配，说明 Log 在该 index 被覆盖了
		if res.Term != term || res.ClientID != op.ClientID || res.RPCID != op.RPCID {
			return OpResult{Err: ErrWrongLeader}
		}
		return res
	case <-time.After(500 * time.Millisecond):
		// 超时
		return OpResult{Err: ErrTimeout}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 创建操作
	op := Op{
		Type:     GET,
		Key:      args.Key,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}
	
	// 统一的等待逻辑
	res := kv.waitForApplied(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 创建操作
	op := Op{
		Type:     PUT,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}
	
	res := kv.waitForApplied(op)
	reply.Err = res.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 创建操作
	op := Op{
		Type:     APPEND,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}
	
	res := kv.waitForApplied(op)
	reply.Err = res.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.

// 辅助函数：判断是否需要生成快照
func (kv *KVServer) ifNeededSnapshot() {
    if kv.maxraftstate == -1 {
        return
    }

	DPrintf("Server %d: Snapshotting! Size: %d, Max: %d, Index: %d", 
	kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, kv.LastAppliedIndex)

    // 这里的阈值一般设为 Maxraftstate 的 90% 或接近值
    if kv.persister.RaftStateSize() > (kv.maxraftstate * 9 / 10){
        // 开始生成快照
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.Data)
		e.Encode(kv.LastOp)
		e.Encode(kv.LastAppliedIndex)
		
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.LastAppliedIndex, snapshot)
    }
}

// 辅助函数：快照恢复
func (kv *KVServer) restoreSnapshot(snapshot []byte) {
    if snapshot == nil || len(snapshot) < 1 {
        return
    }

    r := bytes.NewBuffer(snapshot)
    d := labgob.NewDecoder(r)
    
    var data map[string]string
    var lastOp map[int64]OpResult
	var lastAppliedIndex int

    // 只需要解码这两个关键状态
    if d.Decode(&data) != nil || 
	d.Decode(&lastOp) != nil ||
	d.Decode(&lastAppliedIndex) != nil {
		// 错误修正
	} else {
		// if lastAppliedIndex > kv.LastAppliedIndex
		kv.Data = data
		kv.LastOp = lastOp
		kv.LastAppliedIndex = lastAppliedIndex
    }
}

// 将applyChannel中的命令应用到状态机中
func (kv *KVServer) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {

			kv.mu.Lock()

			// 如果该命令已经被快照处理过（过期），直接忽略
            if applyMsg.CommandIndex <= kv.LastAppliedIndex {
                kv.mu.Unlock()
                continue
            }

            // 更新 LastAppliedIndex (必须在处理逻辑之前或之后，但必须对所有节点生效)
            kv.LastAppliedIndex = applyMsg.CommandIndex

			// 默认结果
			op := applyMsg.Command.(Op)
			var opRes OpResult
			opRes.ClientID = op.ClientID
			opRes.RPCID = op.RPCID
			opRes.Term = 0

			// 幂等性去重
			// 这里采用最严格的一次性语义：如果已经执行过，直接返回旧结果
			if lastRes, ok := kv.LastOp[op.ClientID]; ok && lastRes.RPCID == op.RPCID {
				opRes = lastRes
				opRes.Err = OK // 确保是 OK
			} else {
				// 执行状态机逻辑
				switch op.Type {
				case "Put":
					kv.Data[op.Key] = op.Value
					opRes.Err = OK
				case "Append":
					kv.Data[op.Key] += op.Value
					opRes.Err = OK
				case "Get":
					if val, ok := kv.Data[op.Key]; ok {
						opRes.Value = val
						opRes.Err = OK
					} else {
						opRes.Err = ErrNoKey
					}
				}
				
				// 更新 lastOp 表
				if op.Type != "Get" {
					kv.LastOp[op.ClientID] = opRes
				}
			}

			// 获取当前的 Term 用于校验
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader {
				opRes.Term = currentTerm
			}

			// 通知等待的 RPC
			if ch, ok := kv.notifyChans[applyMsg.CommandIndex]; ok {
				// 非阻塞发送，防止死锁
				select {
				case ch <- opRes:
				default:
				}
			}

			// 检测是否需要快照
			kv.ifNeededSnapshot()

			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			// 如果快照比当前状态旧，则忽略
			if applyMsg.SnapshotIndex > kv.LastAppliedIndex { 
				kv.restoreSnapshot(applyMsg.Snapshot)
			}
			kv.mu.Unlock()
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.Data = make(map[string]string)
	kv.LastOp = make(map[int64]OpResult)
	kv.notifyChans = make(map[int]chan OpResult)
	kv.LastAppliedIndex = 0
	
	// 从persister中恢复数据（用于重启）
	kv.restoreSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applier()

	return kv
}
