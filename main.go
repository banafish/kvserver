package main

import (
	"kvserver/api"
	"kvserver/config"
	"kvserver/kvraft"
	"kvserver/raft"
	"kvserver/util"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
)

func main() {
	cfg := config.ReadCfg()
	util.SetLogOutputFile(cfg.LogFile)
	util.Debug = cfg.Debug

	rpcCh := make(chan raft.RPC)
	kv := kvraft.StartKVServer(cfg.RaftCfg.PeerAddress, cfg.RaftCfg.Me, api.NewTransport(), rpcCh)

	kvAPI := api.NewKVServerAPI(kv)
	rfAPI := api.NewRaftAPI(kv.GetRaft(), rpcCh)

	if err := rpc.Register(kvAPI); err != nil {
		log.Fatal("register KVServer err", err)
	}
	if err := rpc.Register(rfAPI); err != nil {
		log.Fatal("register raft err", err)
	}
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", "0.0.0.0:"+strings.Split(cfg.RaftCfg.Me, ":")[1])
	if err != nil {
		log.Fatal("listen err", err)
	}
	if err := http.Serve(l, nil); err != nil {
		log.Fatal("serve err", err)
	}
}
