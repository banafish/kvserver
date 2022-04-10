package api

import (
	"fmt"
	"kvserver/kvraft"
	"kvserver/raft"
	"net/rpc"
	"sync"
	"time"

	"github.com/jinzhu/copier"
)

type RaftAPI struct {
	rf    *raft.Raft
	rpcCh chan<- raft.RPC
}

type KVServerAPI struct {
	kv *kvraft.KVServer
}

func NewRaftAPI(rf *raft.Raft, rpcCh chan raft.RPC) *RaftAPI {
	return &RaftAPI{
		rf:    rf,
		rpcCh: rpcCh,
	}
}

func NewKVServerAPI(kv *kvraft.KVServer) *KVServerAPI {
	return &KVServerAPI{kv: kv}
}

func (r *RaftAPI) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	r.handle(args, reply)
	return nil
}

func (r *RaftAPI) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	r.handle(args, reply)
	return nil
}

func (r *RaftAPI) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	r.handle(args, reply)
	return nil
}

func (r *RaftAPI) GetRaftStat(args *raft.GetRaftStatArgs, reply *raft.GetRaftStatReply) error {
	r.rf.GetRaftStat(args, reply)
	return nil
}

func (r *RaftAPI) handle(args interface{}, reply interface{}) {
	req := raft.RPC{
		Args:  args,
		Reply: make(chan interface{}, 1),
	}

	if ae, ok := args.(*raft.AppendEntriesArgs); ok && ae.PrevLogIndex == -1 {
		r.rf.ProcessHeartBeat(req)
	} else {
		r.rpcCh <- req
	}

	rep := <-req.Reply
	copier.Copy(reply, rep)
}

func (s *KVServerAPI) Get(args *kvraft.GetArgs, reply *kvraft.GetReply) error {
	s.kv.Get(args, reply)
	return nil
}

func (s *KVServerAPI) PutAppend(args *kvraft.PutAppendArgs, reply *kvraft.PutAppendReply) error {
	s.kv.PutAppend(args, reply)
	return nil
}

func (s *KVServerAPI) GetServerStat(args *kvraft.GetServerStatArgs, reply *kvraft.GetServerStatReply) error {
	s.kv.GetServerStat(args, reply)
	return nil
}

type trans struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client
}

func NewTransport() raft.Transport {
	return &trans{
		clients: map[string]*rpc.Client{},
	}
}

func (t *trans) RequestVote(target string, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	return t.sendRPCRequest(target, "RaftAPI.RequestVote", args, reply)
}

func (t *trans) AppendEntries(target string, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	return t.sendRPCRequest(target, "RaftAPI.AppendEntries", args, reply)
}

func (t *trans) InstallSnapshot(target string, args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	return t.sendRPCRequest(target, "RaftAPI.InstallSnapshot", args, reply)
}

func (t *trans) sendRPCRequest(target string, serviceMethod string, args interface{}, reply interface{}) error {
	s, err := t.getRPCClient(target)
	if err != nil {
		return err
	}
	e := make(chan error)
	go func() {
		e <- s.Call(serviceMethod, args, reply)
	}()
	select {
	case err = <-e:
		if err != nil {
			// 发送错误，清空重连
			t.mu.Lock()
			t.clients[target] = nil
			t.mu.Unlock()
		}
		return err
	case <-time.After(500 * time.Millisecond):
		return fmt.Errorf("err reply")
	}
}

func (t *trans) getRPCClient(target string) (*rpc.Client, error) {
	t.mu.Lock()
	c := t.clients[target]
	t.mu.Unlock()
	if c != nil {
		return c, nil
	}
	c, err := rpc.DialHTTP("tcp", target)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.clients[target] = c
	t.mu.Unlock()
	return c, nil
}
