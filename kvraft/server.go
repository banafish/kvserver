package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"kvserver/raft"
	"kvserver/util"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	ClientID string
	Seq      int
	OpType   OpType
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      string
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int

	stateMachine       map[string]string
	executeMap         map[string]int
	lastApplyIndex     int
	lastApplyIndexCond *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 当前节点不是 leader
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	waitFreeIndex := kv.rf.WaitFreeIndex()
	kv.mu.Lock()
	// 等待日志应用到 waitFreeIndex
	for kv.lastApplyIndex < waitFreeIndex {
		kv.lastApplyIndexCond.Wait()
	}
	reply.Value = kv.stateMachine[args.Key]
	kv.mu.Unlock()
	util.DPrintf("%v %v get args %+v reply %+v", util.DInfo, kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}

	kv.mu.Lock()
	seq := kv.executeMap[op.ClientID]
	kv.mu.Unlock()
	// 去重
	if seq >= op.Seq {
		return
	}

	replyCh := make(chan interface{})
	s := &raft.StartCommand{
		Command: op,
		Reply:   replyCh,
	}
	// 交给 Raft 共识
	_, _, isLeader := kv.rf.Start(s)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderID = s.LeaderID
		// 超，这里忘记 return 了，导致下面 <-replyCh 被塞住了
		return
	}

	// 等待回复
	res := <-replyCh
	if v, ok := res.(PutAppendReply); ok {
		reply.Err = v.Err
		reply.LeaderID = v.LeaderID
	} else {
		// 又忘记处理这种情况了，超
		reply.LeaderID = s.LeaderID
		reply.Err = ErrWrongLeader
	}
	util.DPrintf("%v %v PutAppend op %+v res %+v", util.DInfo, kv.me, op, res)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []string, me string, trans raft.Transport, rpcCh chan raft.RPC) *KVServer {
	gob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = 1024

	kv.stateMachine = make(map[string]string)
	kv.executeMap = make(map[string]int)
	kv.lastApplyIndexCond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.rf = raft.New(servers, me, trans, raft.MakePersister(), kv.applyCh, rpcCh)

	go kv.applier(kv.applyCh)
	go kv.generateSnapshot()

	return kv
}

func (kv *KVServer) GetRaft() *raft.Raft {
	return kv.rf
}

func (kv *KVServer) applier(applyCh chan raft.ApplyMsg) {
	for m := range applyCh {
		util.DPrintf("%v %v applier ApplyMsg %+v", util.DInfo, kv.me, m)
		if m.SnapshotValid {
			kv.handleSnapshot(m)
			// 写成了return，de了几小时的bug，艹
			continue
		}
		kv.handleOp(m)
	}
}

func (kv *KVServer) handleOp(m raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if m.CommandIndex <= kv.lastApplyIndex {
		return
	}
	kv.lastApplyIndex = m.CommandIndex
	kv.lastApplyIndexCond.Broadcast()

	if !m.CommandValid {
		return
	}
	cmd := m.Command
	var startCmd *raft.StartCommand
	if s, ok := m.Command.(*raft.StartCommand); ok {
		startCmd = s
		cmd = s.Command
	}

	defer func() {
		if startCmd != nil && startCmd.Reply != nil {
			startCmd.Reply <- PutAppendReply{}
		}
	}()

	op, ok := cmd.(Op)
	if !ok {
		return
	}

	util.DPrintf("%v %v applier kv.executeMap[%v] = %v", util.DInfo, kv.me, op.ClientID, kv.executeMap[op.ClientID])
	// 这里对客户端进行了限制，对于同一个 ClientID，只有当前命令执行成功了才会递增 seq 执行下一个命令
	if op.Seq <= kv.executeMap[op.ClientID] {
		return
	}

	switch op.OpType {
	case OpTypePut:
		kv.stateMachine[op.Key] = op.Value
	case OpTypeAppend:
		kv.stateMachine[op.Key] = kv.stateMachine[op.Key] + op.Value
	case OpTypeDelete:
		delete(kv.stateMachine, op.Key)
	}
	kv.executeMap[op.ClientID] = op.Seq
}

func (kv *KVServer) handleSnapshot(m raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
		return
	}
	r := bytes.NewBuffer(m.Snapshot)
	d := gob.NewDecoder(r)
	s := make(map[string]string)
	e := make(map[string]int)
	d.Decode(&s)
	d.Decode(&e)
	kv.stateMachine = s
	kv.executeMap = e
	kv.lastApplyIndex = m.SnapshotIndex
}

func (kv *KVServer) generateSnapshot() {
	for !kv.killed() {
		time.Sleep(500 * time.Millisecond)
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			e.Encode(kv.stateMachine)
			e.Encode(kv.executeMap)
			snapshot := w.Bytes()
			lastApplyIndex := kv.lastApplyIndex
			kv.mu.Unlock()
			kv.rf.Snapshot(lastApplyIndex, snapshot)
			util.DPrintf("%v %v generateSnapshot lastApplyIndex %d", util.DInfo, kv.me, lastApplyIndex)
		}
	}
}

type GetServerStatArgs struct {
}

type GetServerStatReply struct {
	Stat string
}

func (kv *KVServer) GetServerStat(args *GetServerStatArgs, reply *GetServerStatReply) {
	format := `me %v
lastApplyIndex %v
stateMachine %v
executeMap %v
`
	kv.mu.Lock()
	reply.Stat = fmt.Sprintf(format, kv.me, kv.lastApplyIndex, kv.stateMachine, kv.executeMap)
	kv.mu.Unlock()
}
