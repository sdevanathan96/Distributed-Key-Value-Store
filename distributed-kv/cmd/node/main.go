package main

import (
	"distributed-kv/internal/raft"
	"distributed-kv/internal/server"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.String("node-id", "", "this node's id")
	raftAddr := flag.String("raft-addr", "", "host:port for Raft RPC (all shards share it); the client port is this + 1")
	dataDir := flag.String("data-dir", "", "data directory")
	peersRaw := flag.String("peers", "", "comma-separated id=addr list of OTHER nodes")
	numShards := flag.Int("shards", 1, "number of shards; every node must use the same value, and it cannot change after first start")
	virtualNodes := flag.Int("vnodes", sharding.DefaultVirtualNodes, "hash ring points per shard; same rules as --shards")
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

	ring, err := sharding.NewHashRing(*numShards, *virtualNodes)
	if err != nil {
		log.Fatalf("build hash ring: %v", err)
	}
	if err := sharding.EnsureLayout(*dataDir, ring.Layout()); err != nil {
		log.Fatalf("shard layout: %v", err)
	}

	// Every shard is its own Raft group spanning the same nodes, so each gets
	// the same node id and peer map, and its own data dir.
	shards := make([]*shard.Shard, *numShards)
	for i := range shards {
		rcfg := raft.RaftConfig{
			NodeID:             *nodeID,
			Peers:              peers,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		}
		sh, err := shard.NewShard(shard.Config{
			ID:         i,
			DataDir:    filepath.Join(*dataDir, fmt.Sprintf("shard-%d", i)),
			RaftConfig: rcfg,
		})
		if err != nil {
			log.Fatalf("create shard %d: %v", i, err)
		}
		shards[i] = sh
	}

	srv, err := server.NewServer(shards, ring)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}
	if err := srv.StartRaftServer(*raftAddr); err != nil {
		log.Fatalf("start raft server: %v", err)
	}
	srv.Start()

	clientAddr, err := server.RaftAddrToClientAddr(*raftAddr)
	if err != nil {
		log.Fatalf("derive client addr: %v", err)
	}
	kv, err := server.NewKVServer(srv, clientAddr)
	if err != nil {
		log.Fatalf("create kv server: %v", err)
	}
	log.Printf("node %s up: raft=%s client=%s peers=%d shards=%d", *nodeID, *raftAddr, clientAddr, len(peers), *numShards)
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
	log.Printf("node %s stopped", *nodeID)
}
