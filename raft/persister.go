package raft

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

const (
	raftStatFileName = "rf.state"
	raftLogFileName  = "rf.log"
	snapshotFileName = "rf.snapshot"
)

type Persister struct {
	logs    []Log
	offset  int
	logLock sync.RWMutex
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) RaftStateSize() int {
	return ps.getLogLen()
}

func (ps *Persister) ReadSnapshot() []byte {
	if !ps.exists(snapshotFileName) {
		return nil
	}
	snapshot, err := ioutil.ReadFile(snapshotFileName)
	if err != nil {
		log.Fatal("从磁盘读取snapshot错误", err)
	}
	return snapshot
}

func (ps *Persister) saveSnapshot(snapshot []byte) {
	if err := ioutil.WriteFile(snapshotFileName, snapshot, 0644); err != nil {
		log.Fatal("持久化snapshot错误", err)
	}
}

func (ps *Persister) saveRaftState(currentTerm int32, votedFor string) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(currentTerm)
	e.Encode(votedFor)
	data := w.Bytes()
	if err := ioutil.WriteFile(raftStatFileName, data, 0644); err != nil {
		log.Fatal("持久化raft状态错误", err)
	}
}

func (ps *Persister) readRaftState() (currentTerm int32, votedFor string) {
	if !ps.exists(raftStatFileName) {
		return 0, ""
	}
	raftstate, err := ioutil.ReadFile(raftStatFileName)
	if err != nil {
		log.Fatal("从磁盘读取raft状态错误", err)
	}
	r := bytes.NewBuffer(raftstate)
	d := gob.NewDecoder(r)
	d.Decode(&currentTerm)
	d.Decode(&votedFor)

	if ps.exists(raftLogFileName) {
		logstate, err := ioutil.ReadFile(raftLogFileName)
		if err != nil {
			log.Fatal("从磁盘读取raft log错误", err)
		}
		r = bytes.NewBuffer(logstate)
		d = gob.NewDecoder(r)
		ps.logLock.Lock()
		d.Decode(&ps.offset)
		d.Decode(&ps.logs)
		ps.logLock.Unlock()
	}
	return
}

func (ps *Persister) getLogOffset() int {
	ps.logLock.RLock()
	defer ps.logLock.RUnlock()
	return ps.offset
}

func (ps *Persister) appendLog(l Log) int {
	ps.logLock.Lock()
	defer ps.logLock.Unlock()
	ps.logs = append(ps.logs, l)
	ps.saveLogs()
	return len(ps.logs) - 1 + ps.offset
}

func (ps *Persister) getLog(idx int) Log {
	ps.logLock.RLock()
	defer ps.logLock.RUnlock()
	return ps.logs[ps.idx(idx)]
}

func (ps *Persister) idx(idx int) int {
	return idx - ps.offset
}

func (ps *Persister) getFirstLog() Log {
	ps.logLock.RLock()
	defer ps.logLock.RUnlock()
	return ps.logs[0]
}

func (ps *Persister) getLastLogIndexAndTerm() (int, int32) {
	ps.logLock.RLock()
	defer ps.logLock.RUnlock()
	idx := len(ps.logs) - 1
	return idx + ps.offset, ps.logs[idx].Term
}

func (ps *Persister) getLogs(start int) []Log {
	ps.logLock.RLock()
	defer ps.logLock.RUnlock()
	idx := ps.idx(start)
	if idx < 0 {
		return nil
	}
	return ps.logs[idx:]
}

func (ps *Persister) getLogLen() int {
	ps.logLock.RLock()
	defer ps.logLock.RUnlock()
	return len(ps.logs)
}

func (ps *Persister) truncateLog(offset int, lastIncludedLog Log) {
	ps.logLock.Lock()
	defer ps.logLock.Unlock()
	i := ps.idx(offset)
	if i >= len(ps.logs) || (lastIncludedLog.Term != -1 && lastIncludedLog.Term != ps.logs[i].Term) {
		ps.logs = []Log{lastIncludedLog}
	} else {
		ps.logs = ps.logs[i:]
	}
	ps.offset = offset
	ps.saveLogs()
}

func (ps *Persister) setLogs(start int, logs []Log) {
	l := len(logs)
	if l == 0 {
		return
	}
	ps.logLock.Lock()
	defer ps.logLock.Unlock()
	idx := ps.idx(start) + l
	if len(ps.logs) < idx || logs[l-1].Term != ps.logs[idx-1].Term {
		ps.logs = append(ps.logs[:ps.idx(start)], logs...)
		ps.saveLogs()
	}
}

func (ps *Persister) saveLogs() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(ps.offset)
	e.Encode(ps.logs)
	data := w.Bytes()
	if err := ioutil.WriteFile(raftLogFileName, data, 0644); err != nil {
		log.Fatal("持久化raft log错误", err)
	}
}

func (ps *Persister) exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
