package raft

type Transport interface {
	RequestVote(target string, args *RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(target string, args *AppendEntriesArgs, reply *AppendEntriesReply) error
	InstallSnapshot(target string, args *InstallSnapshotArgs, reply *InstallSnapshotReply) error
}
