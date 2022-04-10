package raft

import (
	"fmt"
	"kvserver/util"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Raft struct {
	peers     []string
	persister *Persister
	transport Transport
	me        string
	dead      int32

	state             int32
	currentTerm       int32
	votedFor          string
	leaderID          string
	heartBeatInterval time.Duration
	randomTime        time.Duration
	electionTimeout   time.Duration
	lastContact       time.Time
	isLeaderUpdate    bool
	lastContactLock   sync.Mutex

	rpcCh chan RPC

	lastCommitIndex int32
	commitCh        chan struct{}

	startCommandCh chan *StartCommand

	needReply map[int]*StartCommand

	matchIndex     map[string]int
	matchIndexLock sync.Mutex

	applyCh        chan<- ApplyMsg
	lastApplyIndex int32

	// leaseDuration = electionTime / clockDriftBound
	leaseDuration time.Duration
	// 时钟漂移界限，大于 1
	clockDriftBound float32
	waitFreeIndex   int32

	replicationContextMap map[string]*replicationContext
}

const (
	raftStateFollower  int32 = 0
	raftStateCandidate int32 = 1
	raftStateLeader    int32 = 2
)

func (rf *Raft) run() {
	for !rf.killed() {
		switch rf.getState() {
		case raftStateLeader:
			rf.runAsLeader()
		case raftStateCandidate:
			rf.runAsCandidate()
		case raftStateFollower:
			rf.runAsFollower()
		default:
			log.Fatalf("err raft state %v", rf.getState())
		}
	}
}

func (rf *Raft) runAsLeader() {
	lastLogIndex, currentTerm := rf.persister.getLastLogIndexAndTerm()
	rf.leaderID = ""
	rf.replicationContextMap = make(map[string]*replicationContext)
	rf.commitCh = make(chan struct{}, 1)
	stopReplicateCh := make(chan struct{})
	becomeFollowerCh := make(chan struct{}, 1)
	var canReplicateChs []chan<- struct{}
	for _, target := range rf.peers {
		rf.matchIndex[target] = 0
		if target == rf.me {
			continue
		}
		canReplicateCh := make(chan struct{}, 1)
		canReplicateChs = append(canReplicateChs, canReplicateCh)
		replCtx := &replicationContext{
			target:           target,
			currentTerm:      currentTerm,
			nextIndex:        lastLogIndex,
			stopReplicateCh:  stopReplicateCh,
			becomeFollowerCh: becomeFollowerCh,
			canReplicateCh:   canReplicateCh,
		}
		rf.replicationContextMap[target] = replCtx
		go rf.replication(replCtx)
	}
	util.DPrintf("%v S%v run as leader rf %+v", util.DLeader, rf.me, rf)
	defer func() {
		close(stopReplicateCh)
		for k, v := range rf.needReply {
			v.LeaderID = rf.leaderID
			v.Reply <- fmt.Errorf("err leader")
			delete(rf.needReply, k)
		}
	}()

	rf.asyncNotifyAllCh(canReplicateChs)

	leaseTimer := time.After(rf.leaseDuration)
	for !rf.killed() && rf.getState() == raftStateLeader {
		select {
		case <-becomeFollowerCh:
			util.DPrintf("%v S%v become follower term %d", util.DLeader, rf.me, currentTerm)
			rf.setState(raftStateFollower)
		case rpc := <-rf.rpcCh:
			rf.processRPC(rpc)
		case <-rf.commitCh:
			// 通知更新 follower 的commitIndex
			rf.asyncNotifyAllCh(canReplicateChs)
			rf.applyLogToCh()
		case s := <-rf.startCommandCh:
			s.Index = rf.persister.appendLog(Log{
				Term:    currentTerm,
				Command: s.Command,
			})
			s.Term = currentTerm
			s.IsLeader <- true
			rf.asyncNotifyAllCh(canReplicateChs)
			if s.Reply != nil {
				rf.needReply[s.Index] = s
			}
			util.DPrintf("%v S%d handle startCommandCh %+v", util.DLeader, rf.me, s)
		case <-leaseTimer:
			maxDiff := rf.checkLease()
			leaseTimer = time.After(rf.leaseDuration - maxDiff)
		}
	}
}

func (rf *Raft) runAsCandidate() {
	electionTimer := rf.randomTimeout(rf.electionTimeout)
	currentTerm := rf.getCurrentTerm() + 1
	rf.votedFor = rf.me
	rf.setCurrentTerm(currentTerm)
	util.DPrintf("%v S%v run as candidate rf %+v", util.DInfo, rf.me, rf)
	voteCh := rf.startElection(currentTerm)
	cnt := 0
	for !rf.killed() && rf.getState() == raftStateCandidate {
		select {
		case <-electionTimer:
			return
		case rpc := <-rf.rpcCh:
			rf.processRPC(rpc)
		case s := <-rf.startCommandCh:
			s.LeaderID = rf.leaderID
			s.IsLeader <- false
		case reply := <-voteCh:
			if reply.Term > currentTerm {
				util.DPrintf("%v S%v candidate become follower", util.DVote, rf.me)
				rf.setCurrentTerm(reply.Term)
				rf.setState(raftStateFollower)
				return
			}
			if reply.VoteGranted {
				cnt++
				if rf.isMoreThanHalf(cnt) {
					util.DPrintf("%v S%v candidate become leader", util.DVote, rf.me)
					lastLogIndex := rf.persister.appendLog(Log{
						Term:    currentTerm,
						Command: nil,
					})
					// 先设置 waitFreeIndex 在切 state，防止 leaseRead 读到错误的 waitFreeIndex
					rf.setWaitFreeIndex(lastLogIndex)
					rf.setState(raftStateLeader)
				}
			}
		}
	}
}

func (rf *Raft) runAsFollower() {
	util.DPrintf("%v S%v run as follower rf %+v", util.DInfo, rf.me, rf)
	heartBeatTimer := rf.randomTimeout(rf.electionTimeout)
	for !rf.killed() && rf.getState() == raftStateFollower {
		select {
		case <-heartBeatTimer:
			heartBeatTimer = rf.randomTimeout(rf.electionTimeout)
			lastContact, _ := rf.getLastContact()
			if time.Now().Sub(lastContact) < rf.electionTimeout {
				continue
			}
			util.DPrintf("%v S%v follower become candidate", util.DInfo, rf.me)
			rf.setState(raftStateCandidate)
		case rpc := <-rf.rpcCh:
			rf.processRPC(rpc)
		case s := <-rf.startCommandCh:
			s.LeaderID = rf.leaderID
			s.IsLeader <- false
		}
	}
}

func (rf *Raft) replication(replCtx *replicationContext) {
	stopHeartBeatCh := make(chan struct{})
	defer func() {
		close(stopHeartBeatCh)
	}()
	go rf.sendHeartBeat(replCtx, stopHeartBeatCh)

	for !rf.killed() {
		select {
		case <-replCtx.stopReplicateCh:
			util.DPrintf("%v S%v stop replicate %+v", util.DLeader, rf.me, replCtx)
			return
		case <-replCtx.canReplicateCh:
			logs := rf.persister.getLogs(replCtx.nextIndex - 1)
			if len(logs) == 0 {
				rf.sendSnapshot(replCtx)
			} else {
				// 没复制成功通知继续复制
				if !rf.replicateTo(replCtx, logs) {
					rf.asyncNotifyCh(replCtx.canReplicateCh)
				}
			}
		}
	}
}

func (rf *Raft) sendSnapshot(replCtx *replicationContext) {
	var args InstallSnapshotArgs
	var reply InstallSnapshotReply
	args.Snapshot = rf.persister.ReadSnapshot()
	args.LastIncludedIndex = rf.persister.getLogOffset()
	args.LastIncludedLog = rf.persister.getFirstLog()
	args.Term = replCtx.currentTerm

	util.DPrintf("%v S%v send snapshot to S%v args %+v", util.DLeader, rf.me, replCtx.target, args)
	if err := rf.transport.InstallSnapshot(replCtx.target, &args, &reply); err != nil {
		util.DPrintf("%v S%v send snapshot to S%v err %+v", util.DLeader, rf.me, replCtx.target, err)
		return
	}

	if reply.Term > replCtx.currentTerm {
		rf.asyncNotifyCh(replCtx.becomeFollowerCh)
		return
	}
	replCtx.updateLastContact()
	replCtx.nextIndex = reply.ApplyIndex + 1
	util.DPrintf("%v S%v send snapshot to S%v success reply %+v", util.DLeader, rf.me, replCtx.target, reply)
}

func (rf *Raft) replicateTo(replCtx *replicationContext, logs []Log) bool {
	var args AppendEntriesArgs
	args.Term = replCtx.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = replCtx.nextIndex - 1
	args.PrevLogTerm = logs[0].Term
	args.LeaderCommitIndex = rf.getLastCommitIndex()
	args.Entries = logs[1:]

	util.DPrintf("%v S%v replicate to S%v args %+v", util.DLeader, rf.me, replCtx.target, args)
	var reply AppendEntriesReply
	if err := rf.transport.AppendEntries(replCtx.target, &args, &reply); err != nil {
		util.DPrintf("%v S%v replicate to S%v err %+v", util.DLeader, rf.me, replCtx.target, err)
		return false
	}
	if reply.Term > replCtx.currentTerm {
		rf.asyncNotifyCh(replCtx.becomeFollowerCh)
		return true
	}

	replCtx.updateLastContact()
	if !reply.Success {
		replCtx.nextIndex = reply.ConflictFirstIndex
		return false
	}

	matchIndex := args.PrevLogIndex + len(args.Entries)
	replCtx.nextIndex = matchIndex + 1

	rf.matchIndexLock.Lock()
	defer rf.matchIndexLock.Unlock()
	lastLogIndex := args.PrevLogIndex + len(args.Entries)
	if lastLogIndex > rf.matchIndex[rf.me] {
		rf.matchIndex[rf.me] = lastLogIndex
	}
	rf.matchIndex[replCtx.target] = matchIndex
	commitIndex := rf.calculate()
	if args.LeaderCommitIndex < commitIndex && replCtx.currentTerm == rf.persister.getLog(commitIndex).Term {
		rf.setLastCommitIndex(commitIndex)
		rf.asyncNotifyCh(rf.commitCh)
	}
	util.DPrintf("%v S%v replicate to S%v success reply %+v commitIndex %d lastCommitIndex %d",
		util.DLeader, rf.me, replCtx.target, reply, commitIndex, args.LeaderCommitIndex)
	return true
}

func (rf *Raft) calculate() int {
	var matched []int
	for _, idx := range rf.matchIndex {
		matched = append(matched, idx)
	}
	sort.Ints(matched)
	commitIndex := matched[(len(matched)-1)/2]
	util.DPrintf("%v S%v calculate commitIndex %d %v", util.DLeader, rf.me, commitIndex, rf.matchIndex)
	return commitIndex
}

func (rf *Raft) sendHeartBeat(replCtx *replicationContext, stopHeartBeatCh chan struct{}) {
	for !rf.killed() {
		select {
		case <-stopHeartBeatCh:
			util.DPrintf("%v S%v stop send heartbeat to S%v term %d", util.DLeader, rf.me, replCtx.target, replCtx.currentTerm)
			return
		case <-time.After(rf.heartBeatInterval):
		}
		util.DPrintf("%v S%v send heartbeat to S%v term %d", util.DLeader, rf.me, replCtx.target, replCtx.currentTerm)
		args := AppendEntriesArgs{
			Term:         replCtx.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: -1,
		}
		var reply AppendEntriesReply
		err := rf.transport.AppendEntries(replCtx.target, &args, &reply)
		if err == nil {
			replCtx.updateLastContact()
		}
		util.DPrintf("%v S%v send heartbeat to S%v term %d err %+v", util.DLeader, rf.me, replCtx.target, replCtx.currentTerm, err)
	}
}

func (rf *Raft) checkLease() time.Duration {
	contacted := 0
	var maxDiff time.Duration
	now := time.Now()
	for _, target := range rf.peers {
		if target == rf.me {
			contacted++
			continue
		}
		r := rf.replicationContextMap[target]
		diff := now.Sub(r.getLastContact())
		if diff <= rf.leaseDuration {
			contacted++
			if diff > maxDiff {
				maxDiff = diff
			}
		}
	}

	// 没有在租期内联系到足够多的 peer，租期结束
	if contacted <= len(rf.peers)/2 {
		util.DPrintf("%v S%v lease timeout term %d", util.DLeader, rf.me, rf.getCurrentTerm())
		rf.setState(raftStateFollower)
	}
	return maxDiff
}

func (rf *Raft) IsLeader() bool {
	return rf.getState() == raftStateLeader
}

func (rf *Raft) asyncNotifyCh(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (rf *Raft) asyncNotifyAllCh(chs []chan<- struct{}) {
	for _, ch := range chs {
		rf.asyncNotifyCh(ch)
	}
}

func (rf *Raft) processRPC(rpc RPC) {
	switch args := rpc.Args.(type) {
	case *RequestVoteArgs:
		rf.requestVote(rpc, args)
	case *AppendEntriesArgs:
		rf.appendEntries(rpc, args)
	case *InstallSnapshotArgs:
		rf.installSnapshot(rpc, args)
	default:
		rpc.Reply <- fmt.Errorf("err rpc")
	}
}

func (rf *Raft) installSnapshot(rpc RPC, args *InstallSnapshotArgs) {
	currentTerm := rf.getCurrentTerm()
	lastApplyIndex := rf.getLastApplyIndex()
	reply := &InstallSnapshotReply{
		Term:       currentTerm,
		ApplyIndex: lastApplyIndex,
	}
	defer func() {
		util.DPrintf("%v S%v handle installSnapshot args %+v reply %+v", util.DSnap, rf.me, args, reply)
		rpc.Reply <- reply
	}()

	rf.updateLastContact(true)

	if lastApplyIndex >= args.LastIncludedIndex || args.Term < currentTerm {
		return
	}
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  int(args.LastIncludedLog.Term),
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.persister.truncateLog(args.LastIncludedIndex, args.LastIncludedLog)
	reply.ApplyIndex = args.LastIncludedIndex
	rf.setLastApplyIndex(args.LastIncludedIndex)
	rf.setState(raftStateFollower)
	if currentTerm != args.Term {
		rf.setCurrentTerm(args.Term)
	}
	rf.persister.saveSnapshot(args.Snapshot)
}

func (rf *Raft) appendEntries(rpc RPC, args *AppendEntriesArgs) {
	currentTerm := rf.getCurrentTerm()
	reply := &AppendEntriesReply{
		Term:               currentTerm,
		Success:            false,
		ConflictFirstIndex: 0,
	}
	defer func() {
		util.DPrintf("%v S%v handle appendEntries args %+v reply %+v", util.DInfo, rf.me, args, reply)
		rpc.Reply <- reply
	}()
	if args.Term < currentTerm {
		return
	}

	rf.updateLastContact(true)
	rf.leaderID = args.LeaderID
	rf.setState(raftStateFollower)
	if args.Term != currentTerm {
		rf.setCurrentTerm(args.Term)
	}
	offset := rf.persister.getLogOffset()
	if args.PrevLogIndex < offset {
		reply.ConflictFirstIndex = offset + 1
		return
	}

	lastLogIndex, _ := rf.persister.getLastLogIndexAndTerm()
	if lastLogIndex >= args.PrevLogIndex && rf.persister.getLog(args.PrevLogIndex).Term == args.PrevLogTerm {
		reply.Success = true
		rf.persister.setLogs(args.PrevLogIndex+1, args.Entries)
		commitIndex := args.LeaderCommitIndex
		if args.PrevLogIndex+len(args.Entries) < commitIndex {
			commitIndex = args.PrevLogIndex + len(args.Entries)
		}
		if rf.getLastCommitIndex() < commitIndex {
			rf.setLastCommitIndex(commitIndex)
			rf.applyLogToCh()
		}
	} else {
		if lastLogIndex < args.PrevLogIndex {
			reply.ConflictFirstIndex = lastLogIndex + 1
		} else {
			i := args.PrevLogIndex - 1
			offset := rf.persister.getLogOffset()

			pTerm := rf.persister.getLog(args.PrevLogIndex).Term
			for i > offset && rf.persister.getLog(i).Term == pTerm {
				i--
			}
			reply.ConflictFirstIndex = i + 1
		}
	}
}

func (rf *Raft) requestVote(rpc RPC, args *RequestVoteArgs) {
	currentTerm := rf.getCurrentTerm()
	reply := &RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: false,
	}
	lastLogIndex, lastLogTerm := rf.persister.getLastLogIndexAndTerm()
	// 保证 leader lease 的安全性和解决非对称网络分区
	lastContact, isLeaderUpdate := rf.getLastContact()
	canVote := time.Now().Sub(lastContact) > rf.electionTimeout || !isLeaderUpdate
	defer func() {
		util.DPrintf("%v S%v handle requestVote args %+v reply %+v lastLogIndex %d lastLogTerm %d canVote %v",
			util.DInfo, rf.me, args, reply, lastLogIndex, lastLogTerm, canVote)
		rpc.Reply <- reply
	}()

	if args.Term < currentTerm || !canVote {
		return
	}

	if args.Term == currentTerm {
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.updateLastContact(false)
		}
		return
	}
	if args.Term != currentTerm {
		rf.setCurrentTerm(args.Term)
	}
	rf.setState(raftStateFollower)
	// 投票限制
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	// 只有当投票给其它候选人了才刷新选举超时
	rf.updateLastContact(false)
	rf.votedFor = args.CandidateId
	rf.persister.saveRaftState(args.Term, rf.votedFor)
	reply.VoteGranted = true
}

func (rf *Raft) startElection(term int32) <-chan RequestVoteReply {
	res := make(chan RequestVoteReply, len(rf.peers))
	lastLogIndex, lastLogTerm := rf.persister.getLastLogIndexAndTerm()
	util.DPrintf("%v S%v startElection", util.DVote, rf.me)
	for _, i := range rf.peers {
		if i == rf.me {
			res <- RequestVoteReply{
				Term:        term,
				VoteGranted: true,
			}
			continue
		}
		go func(target string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if err := rf.transport.RequestVote(target, &args, &reply); err != nil {
				return
			}
			res <- reply
		}(i)
	}
	return res
}

func (rf *Raft) applyLogToCh() {
	lastCommitIndex := rf.getLastCommitIndex()
	lastApplyIndex := rf.getLastApplyIndex()
	if lastApplyIndex < lastCommitIndex {
		util.DPrintf("%v S%v applyLogToCh start idx %d end %d", util.DInfo, rf.me, lastApplyIndex+1, lastCommitIndex)
	}
	for i := lastApplyIndex + 1; i <= lastCommitIndex; i++ {
		var cmd interface{}
		if v, ok := rf.needReply[i]; ok {
			cmd = v
			delete(rf.needReply, i)
		} else {
			l := rf.persister.getLog(i)
			cmd = l.Command
		}

		valid := cmd != nil
		rf.applyCh <- ApplyMsg{
			CommandValid: valid,
			Command:      cmd,
			CommandIndex: i,
		}
	}
	rf.setLastApplyIndex(lastCommitIndex)
}

func (rf *Raft) ProcessHeartBeat(rpc RPC) {
	if args, ok := rpc.Args.(*AppendEntriesArgs); ok {
		rf.appendEntries(rpc, args)
	} else {
		rpc.Reply <- fmt.Errorf("expected type *AppendEntriesArgs")
	}
}

func (rf *Raft) isMoreThanHalf(cnt int) bool {
	return cnt > len(rf.peers)/2
}

func (rf *Raft) randomTimeout(d time.Duration) <-chan time.Time {
	extra := time.Duration(rand.Int63()) % rf.randomTime
	return time.After(d + extra)
}

func (rf *Raft) WaitFreeIndex() int {
	return int(atomic.LoadInt32(&rf.waitFreeIndex))
}

func (rf *Raft) setWaitFreeIndex(idx int) {
	atomic.StoreInt32(&rf.waitFreeIndex, int32(idx))
}

func (rf *Raft) getLastCommitIndex() int {
	return int(atomic.LoadInt32(&rf.lastCommitIndex))
}

func (rf *Raft) setLastCommitIndex(lastCommitIndex int) {
	atomic.StoreInt32(&rf.lastCommitIndex, int32(lastCommitIndex))
}

func (rf *Raft) setLastApplyIndex(lastApplyIndex int) {
	atomic.StoreInt32(&rf.lastApplyIndex, int32(lastApplyIndex))
}

func (rf *Raft) getLastApplyIndex() int {
	return int(atomic.LoadInt32(&rf.lastApplyIndex))
}

func (rf *Raft) getLastContact() (lastContact time.Time, isLeaderUpdate bool) {
	rf.lastContactLock.Lock()
	defer rf.lastContactLock.Unlock()
	return rf.lastContact, rf.isLeaderUpdate
}

func (rf *Raft) updateLastContact(isLeaderUpdate bool) {
	rf.lastContactLock.Lock()
	defer rf.lastContactLock.Unlock()
	rf.lastContact = time.Now()
	rf.isLeaderUpdate = isLeaderUpdate
}

func (rf *Raft) getState() int32 {
	return atomic.LoadInt32(&rf.state)
}

func (rf *Raft) setState(state int32) {
	atomic.StoreInt32(&rf.state, state)
}

func (rf *Raft) setCurrentTerm(term int32) {
	atomic.StoreInt32(&rf.currentTerm, term)
	rf.persister.saveRaftState(term, rf.votedFor)
}

func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) GetState() (term int, isLeader bool) {
	return int(rf.getCurrentTerm()), rf.getState() == raftStateLeader
}

func (rf *Raft) readPersist() {
	util.DPrintf("%v S%v readPersist", util.DPersist, rf.me)
	rf.currentTerm, rf.votedFor = rf.persister.readRaftState()

	snapShot := rf.persister.ReadSnapshot()
	if len(snapShot) > 0 {
		util.DPrintf("%v S%v 发送snapshot到chan", util.DPersist, rf.me)
		offset := rf.persister.getLogOffset()
		firstLog := rf.persister.getFirstLog()
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapShot,
			SnapshotTerm:  int(firstLog.Term),
			SnapshotIndex: offset,
		}
		// 又忘记更新了，导致数组越界
		rf.setLastApplyIndex(offset)
		util.DPrintf("%v S%v 读取并安装snapshot完毕", util.DPersist, rf.me)
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// 之前忘记加锁了，导致读了旧的lastApplyIndex，安装了旧快照
	if rf.getLastApplyIndex() > lastIncludedIndex {
		return false
	}
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if index <= rf.persister.getLogOffset() {
		return
	}
	util.DPrintf("%v S%v 做snapshot idx %d", util.DSnap, rf.me, index)
	rf.persister.truncateLog(index, Log{Term: -1})
	rf.persister.saveSnapshot(snapshot)
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	if rf.killed() {
		return -1, -1, false
	}
	var s *StartCommand
	if c, ok := command.(*StartCommand); ok {
		s = c
	} else {
		s = &StartCommand{}
		s.Command = command
	}
	s.Term = -1
	s.Index = -1
	s.IsLeader = make(chan bool)
	rf.startCommandCh <- s
	isLeader = <-s.IsLeader
	return s.Index, int(s.Term), isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func New(peers []string, me string, trans Transport,
	persister *Persister, applyCh chan ApplyMsg, rpcCh chan RPC) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.transport = trans
	rf.me = me
	rf.state = raftStateFollower
	rf.leaderID = ""
	rf.heartBeatInterval = 50 * time.Millisecond
	rf.randomTime = 100 * time.Millisecond
	rf.electionTimeout = 500 * time.Millisecond
	rf.rpcCh = rpcCh
	rf.startCommandCh = make(chan *StartCommand, 256)
	rf.needReply = make(map[int]*StartCommand)
	rf.persister.logs = append(rf.persister.logs, Log{
		Term: -1,
	})
	rf.matchIndex = make(map[string]int)
	rf.applyCh = applyCh
	rf.clockDriftBound = 1.1
	rf.leaseDuration = time.Duration(float32(rf.electionTimeout) / rf.clockDriftBound)

	rf.readPersist()

	go rf.run()

	return rf
}

func (rf *Raft) GetRaftStat(args *GetRaftStatArgs, reply *GetRaftStatReply) {
	format := `peers %v
me %v
state %v
currentTerm %v
votedFor %v
leaderID %v
lastCommitIndex %v
replCtx %+v
matchIndex %v
lastApplyIndex %v
offset %v
log %v
`
	var log []Log
	if args.IsPrintLog {
		log = rf.persister.getLogs(rf.persister.getLogOffset())
	}
	reply.Stat = fmt.Sprintf(format, rf.peers, rf.me, rf.getState(), rf.getCurrentTerm(), rf.votedFor, rf.leaderID,
		rf.getLastCommitIndex(), rf.replicationContextMap, rf.matchIndex, rf.getLastApplyIndex(), rf.persister.getLogOffset(), log)
}
