package server

import (
	"errors"
	"fmt"

	"distributed-kv/internal/raft"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
)

// Server is the per process router: it owns this node's shards, routes each
// key to one of them through a sharding.Router, and serves every shard's Raft
// RPCs on one port through a raft.Mux.
type Server struct {
	shards []*shard.Shard
	router sharding.Router
	mux    *raft.Mux
}

// NewServer rejects any shard set the router cannot index safely. The router
// returns a shard ID and ShardFor uses it as a slice index, so the slice must
// hold exactly NumShards() shards with shards[i].ID == i. A wrong count would
// panic inside a request handler, which grpc-go does not recover, and would
// take the node down. A wrong order would silently write keys into the wrong
// Raft group.
func NewServer(shards []*shard.Shard, router sharding.Router) (*Server, error) {
	if len(shards) == 0 {
		return nil, errors.New("server: no shards")
	}
	if router.NumShards() != len(shards) {
		return nil, fmt.Errorf("server: router expects %d shards, got %d", router.NumShards(), len(shards))
	}
	for i, sh := range shards {
		if sh.ID != i {
			return nil, fmt.Errorf("server: shards[%d] has ID %d; shards must be ordered by ID", i, sh.ID)
		}
	}
	return &Server{shards: shards, router: router}, nil
}

// StartRaftServer serves every shard's Raft RPCs on addr. Call before Start so
// peers can reach this node as soon as its elections begin.
func (s *Server) StartRaftServer(addr string) error {
	nodes := make([]*raft.RaftNode, len(s.shards))
	for i, sh := range s.shards {
		nodes[i] = sh.RaftNode()
	}
	mux, err := raft.NewMux(nodes)
	if err != nil {
		return err
	}
	if err := mux.Start(addr); err != nil {
		return err
	}
	s.mux = mux
	return nil
}

func (s *Server) Start() {
	for _, sh := range s.shards {
		sh.Start()
	}
}

// Stop drains the Raft port first so no handler is running when the shards
// stop, then stops every shard and returns the first error.
func (s *Server) Stop() error {
	if s.mux != nil {
		s.mux.Stop()
	}
	var firstErr error
	for _, sh := range s.shards {
		if err := sh.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Server) ShardFor(key []byte) *shard.Shard {
	return s.shards[s.router.ShardFor(key)]
}
