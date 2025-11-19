package raft

// TODO
// 心跳也要进行完整的日志一致性检查
// 心跳和选举分开goroutine
// 单独运行的长协程按顺序在applych上发送已提交的日志项目(推进commitIndex的代码来唤醒apply协程)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	//"runtime/debug"
	"sync"
	"sync/atomic"

	"fmt"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	//"github.com/cosiner/argv"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// 服务器的角色
const (
	FOLLOWER = iota	// 跟随者
	LEADER 			// 领导者
	CANDIDATE		// 候选者
)

var PRINTINF = false	// 打印信息


// ApplyMsg : 当日志条目被commit，该服务器会向m.applyCh发送一个ApplyMsg
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry : 日志条目
type LogEntry struct {
	Term int				// 日志条目的任期号
	Command interface{}		// 日志条目的内容
	TimeStamp int64			// Leader接收该条目的时间
}

// Raft : A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	applyCond  *sync.Cond		  // applychannel的条件变量,表示当前有已经commmit的log可以apply
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 持久状态
	currentTerm int				// 当前任期号
	votedFor int				// 在当前任期号中投票给的候选人,没有投票的为-1
	log []LogEntry				// 存储的日志条目(第一个索引为1)
	applyChannel chan ApplyMsg	// 日志上传通道
	// 易失状态
	role int					// 该服务器的角色,初始都为Follower
	commitIndex int				// 已知已提交的最高日志条目索引值
	lastApplied int				// 已知已应用到状态机的最高日志条目6索引值,且有commmitIndex >= lastApplied
	electionTimer time.Time		// 选举计时
	// Leader的易失状态(在选举后重新初始化)
	nextIndex []int				// 对于每个服务器,要发送给该服务器的下一个日志条目的索引下一个要发送的索引(初始化为Leader的最后一个日志索引+1)
	matchIndex []int			// 对于每个服务器,该服务器已经知道的复制该日志的最高索引值(初始化为0,单增)
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.role == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs : 投票请求参数 example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int 			// 候选人的任期号
	CandidateID int		// 候选者的ID
	LastLogIndex int	// 候选者的最后一条日志的索引值
	LastLogTerm int		// 候选者的最后条日志的任期号
}

// RequestVoteReply : 投票请求回复 example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (3A).
	Term int			// 自身任期号
	VoteGranted bool	// 是否投票给该候选者
}

// RequestVote : 被请求投票的RPC example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("server %d receive RequestVote from %d\n", rf.me, args.CandidateID)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
	// 如果候选者任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果候选者任期更大，转为追随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// 检查投票资格：要么还没投过票，要么已经投给了这个候选者
	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateID)
	
	// 检查日志至少一样新
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	logUpToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if canVote && logUpToDate {
		rf.votedFor = args.CandidateID
		rf.electionTimer = time.Now()
		reply.VoteGranted = true
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

	if PRINTINF {
		if reply.VoteGranted {
			fmt.Printf("server %d vote for %d, term:%d\n", rf.me, args.CandidateID, reply.Term)
		} else {
			fmt.Printf("server %d reject %d, term:%d\n", rf.me, args.CandidateID, reply.Term)
		}
	}
}

// AppendEntriesArgs : 复制日志调用参数
type AppendEntriesArgs struct {
    Term int			// Leader的任期号
    LeaderID int		// Leader的ID
	PrevLogIndex int	// 在新Log之前的日志条目索引
	PrevLogTerm int		// 在新Log之前的日志条目的任期号
	Entries []LogEntry	// 新的日志条目,可能不止一项(如果是心跳则为空)
	LeaderCommit int	// Leader的已提交的日志索引
}

// AppendEntriesReply : 复制日志调用返回参数
type AppendEntriesReply struct {
    Term int			// 当前任期
    Success bool		// 是否成功(Follower的前一条日志条目对应prevLogIndex和prevLogTerm)
	XTerm int  			// 与PrevLogIndex出冲突的日志的任期号，没有冲突则为-1
    XIndex int 			// 该冲突任期的第一条日志的索引
    XLen int    		// 空白的Log槽位数量
}

// AppendEntries : 复制日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果领导者任期小于当前任期，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 收到有效RPC，重置选举超时
	rf.electionTimer = time.Now()

	// 如果领导者任期更大，转为追随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// 检查日志一致性
	if args.PrevLogIndex > len(rf.log)-1 {
		// 日志太短
		reply.Success = false
		reply.XTerm = -1			// 特殊值表示日志太短
		reply.XLen = len(rf.log)	// Follower的当前日志长度
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 任期不匹配
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term  // 冲突位置的任期
        // 找到该任期的第一条日志
        xIndex := args.PrevLogIndex
        for xIndex > 0 && rf.log[xIndex-1].Term == reply.XTerm {
            xIndex--
        }
        reply.XIndex = xIndex
		return
	}

	// 日志匹配成功
	reply.Success = true

	// 处理日志条目，严格处理冲突
	if len(args.Entries) > 0 {
		if PRINTINF {
			fmt.Printf("server %d: log :", rf.me)
			for i := 0 ; i < len(rf.log); i++ {
				fmt.Printf("%d:%v ", rf.log[i].Term, rf.log[i].Command)
			}
			fmt.Printf("recive log :")
			for i := 0 ; i < len(args.Entries); i++ {
				fmt.Printf("%d:%v ", args.Entries[i].Term, args.Entries[i].Command)
			}
			fmt.Printf("\n")
		}

		// 删除冲突的条目并追加新条目
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

		rf.persist()
	}

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	}
}


// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.

// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().

// look at the comments in ../labrpc/labrpc.go for more details.

// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// 更新commitIndex
func (rf *Raft) updateCommitIndex() {
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 1 // 领导者自己
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
		}

		// 确保commitIndex不超过日志数组的实际长度
		if rf.commitIndex >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		}

	}
}

// 广播AppendEntries给所有追随者
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			
			prevLogIndex := rf.nextIndex[server] - 1
			var prevLogTerm int
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			} else {
				prevLogTerm = 0
			}

			entries := make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
			copy(entries, rf.log[rf.nextIndex[server]:])

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}

				if rf.role != LEADER || args.Term != rf.currentTerm {
					return
				}

				if reply.Success {
					rf.matchIndex[server] = prevLogIndex + len(entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex()
				} else {
					if reply.XTerm == -1 {
						// Follower日志太短
						rf.nextIndex[server] = reply.XIndex
					} else {
						// Follower任期冲突
						// 在Leader日志中寻找XTerm的最后一条日志
						lastIndexWithXTerm := -1
						for i := args.PrevLogIndex; i >= 1; i-- {
							if rf.log[i].Term == reply.XTerm {
								lastIndexWithXTerm = i
								break
							}
							if rf.log[i].Term < reply.XTerm {
								break
							}
						}
						
						if lastIndexWithXTerm != -1 {
							// Leader也有这个任期的日志
							rf.nextIndex[server] = lastIndexWithXTerm + 1
						} else {
							// Leader没有这个任期的日志
							rf.nextIndex[server] = reply.XIndex
						}
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.role == LEADER)

	if !isLeader {
		return index, term, isLeader
	}

	// 创建新日志条目
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()

	index = len(rf.log) - 1
	term = rf.currentTerm

	// 立即发送AppendEntries给其他节点
	go rf.broadcastAppendEntries()

	return index, term, isLeader
}


// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.

// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.

func (rf *Raft) startElection() {
	rf.mu.Lock()
	
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer = time.Now()
	rf.persist()
	if PRINTINF {fmt.Printf("server %d start election, its term is %d\n", rf.me, rf.currentTerm)}
	
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	voteCount := 1 // 投给自己
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.role != CANDIDATE || args.Term != rf.currentTerm {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						rf.role = LEADER
						if PRINTINF {fmt.Printf("server %d become leader, its term is %d\n", rf.me, rf.currentTerm)}
						// 初始化领导者状态
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}
						// 立即发送心跳
						go rf.broadcastAppendEntries()
					}
				}
			}
		}(i)
	}
}

// 获取最后日志条目的索引和任期
func (rf *Raft) getLastLogInfo() (int, int) {
	if len(rf.log) == 0 {
		return -1, -1
	}
	lastIndex := len(rf.log) - 1
	return lastIndex, rf.log[lastIndex].Term
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 长时间运行的goroutine
// ticker : 选举检查goroutine
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.role == LEADER {
			rf.mu.Unlock()
			// 领导者发送心跳
			time.Sleep(100 * time.Millisecond)
			rf.broadcastAppendEntries()
			continue
		}

		// 检查选举超时
		electionTimeout := time.Duration(150+rand.Int63()%150) * time.Millisecond
		if time.Since(rf.electionTimer) > electionTimeout {
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// applyLogs : 应用已提交的日志条目
func (rf *Raft) applyLogs() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log)-1  {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyChannel <- applyMsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor	= -1
	rf.log = make([]LogEntry, 1)	// log[0]是哨兵值
	rf.log[0] = LogEntry{
		Term: 0,
		Command: nil,
		TimeStamp: time.Now().UnixNano(),
	}
	rf.applyChannel = applyCh
	rf.role = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimer = time.Now()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogs()

	return rf
}
