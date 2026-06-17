package server

import (
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
)

// Server is a lightweight router that owns N shards and a sharding.Router
// mapping keys to shard IDs. MVP: one shard, SingleShardRouter returns 0.
type Server struct {
	shards []*shard.Shard
	router sharding.Router
}

func NewServer(shards []*shard.Shard, router sharding.Router) *Server {
	return &Server{shards: shards, router: router}
}

func (s *Server) Start() {
	for _, sh := range s.shards {
		sh.Start()
	}
}

func (s *Server) Stop() error {
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
