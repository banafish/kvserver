package raft

import (
	"sync"
	"time"
)

type RequestVoteArgs struct {
	Term         int32
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term              int32
	LeaderID          string
	PrevLogIndex      int
	PrevLogTerm       int32
	Entries           []Log
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term               int32
	Success            bool
	ConflictFirstIndex int
}

type InstallSnapshotArgs struct {
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedLog   Log
	Term              int32
}

type InstallSnapshotReply struct {
	Term       int32
	ApplyIndex int
}

type RPC struct {
	// 指针类型
	Args interface{}
	// 指针类型
	Reply chan interface{}
}

type Log struct {
	Term    int32
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type replicationContext struct {
	target           string
	currentTerm      int32
	nextIndex        int
	lastContact      time.Time
	lastContactLock  sync.RWMutex
	stopReplicateCh  <-chan struct{}
	becomeFollowerCh chan<- struct{}
	canReplicateCh   chan struct{}
}

func (r *replicationContext) updateLastContact() {
	r.lastContactLock.Lock()
	defer r.lastContactLock.Unlock()
	r.lastContact = time.Now()
}

func (r *replicationContext) getLastContact() time.Time {
	r.lastContactLock.RLock()
	defer r.lastContactLock.RUnlock()
	return r.lastContact
}

type StartCommand struct {
	Command  interface{}
	Index    int
	Term     int32
	IsLeader chan bool
	LeaderID string
	Reply    chan interface{}
}

type GetRaftStatArgs struct {
	IsPrintLog bool
}

type GetRaftStatReply struct {
	Stat string
}
