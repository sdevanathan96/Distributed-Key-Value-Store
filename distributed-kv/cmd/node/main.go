package main

import (
	"distributed-kv/internal/raft"
	"distributed-kv/internal/server"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.String("node-id", "", "this node's id")
	raftAddr := flag.String("raft-addr", "", "host:port for Raft RPC")
	dataDir := flag.String("data-dir", "", "data directory")
	peersRaw := flag.String("peers", "", "comma-separated id=addr list of OTHER nodes")
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" || *dataDir == "" {
		log.Fatal("node-id, raft-addr, data-dir are required")
	}

	// parse "node-1=localhost:9010,node-2=localhost:9020" into map[string]string
	peers := map[string]string{}
	if *peersRaw != "" {
		for _, kv := range strings.Split(*peersRaw, ",") {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("bad peer spec %q, want id=addr", kv)
			}
			peers[parts[0]] = parts[1]
		}
	}

	rcfg := raft.RaftConfig{
		NodeID:             *nodeID,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
	sh, err := shard.NewShard(shard.Config{ID: 0, DataDir: *dataDir, RaftConfig: rcfg})
	if err != nil {
		log.Fatalf("create shard: %v", err)
	}

	if err := sh.StartRaftServer(*raftAddr); err != nil {
		log.Fatalf("start raft server: %v", err)
	}

	srv := server.NewServer([]*shard.Shard{sh}, sharding.SingleShardRouter{})
	srv.Start()

	clientAddr, err := server.RaftAddrToClientAddr(*raftAddr)
	if err != nil {
		log.Fatalf("derive client addr: %v", err)
	}
	kv, err := server.NewKVServer(srv, clientAddr)
	log.Printf("node %s up: raft=%s client=%s peers=%d", *nodeID, *raftAddr, clientAddr, len(peers))
	if err != nil {
		log.Fatalf("create kv server: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- kv.Serve()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("shutdown signal received")
	case err := <-serveErr:
		// Serve died on its own (bind lost, etc). Still shut down cleanly.
		log.Printf("kv server stopped unexpectedly: %v", err)
	}

	kv.Stop()
	if err := srv.Stop(); err != nil {
		log.Printf("server stop error: %v", err)
	}
}
