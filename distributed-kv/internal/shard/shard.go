package shard

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	"distributed-kv/internal/raft"
	"distributed-kv/internal/storage"
	"distributed-kv/proto/raftpb"
)

// Shard is one independent unit of consensus and storage. Each shard owns one
// RaftNode and one Engine. Raft owns the apply channel; the shard reads from
// it via raft.ApplyCh(). In a multi shard deployment the Server holds N shards
// and routes by key via a sharding.Router. With one shard the router returns 0
// for every key.
type Shard struct {
	ID        int
	raft      *raft.RaftNode
	engine    *storage.Engine
	waiters   map[string]chan error
	waitersMu sync.Mutex
	wg        sync.WaitGroup
}

// Config is the per shard configuration. Storage lives at DataDir/storage and
// Raft persistence at DataDir/raft so the two never collide.
type Config struct {
	ID         int
	DataDir    string
	RaftConfig raft.RaftConfig
}

// NewShard constructs RaftNode and Engine but does not start them. Storage
// paths are derived from cfg.DataDir for per shard isolation. The apply
// channel is created and owned by the RaftNode, not here.
func NewShard(cfg Config) (*Shard, error) {
	storageCfg := storage.DefaultConfig(filepath.Join(cfg.DataDir, "storage"))
	engine, err := storage.NewEngine(storageCfg)
	if err != nil {
		return nil, fmt.Errorf("shard %d engine: %w", cfg.ID, err)
	}

	rcfg := cfg.RaftConfig
	rcfg.DataDir = filepath.Join(cfg.DataDir, "raft")
	rn, err := raft.NewRaftNode(rcfg)
	if err != nil {
		engine.Close()
		return nil, fmt.Errorf("shard %d raft: %w", cfg.ID, err)
	}

	return &Shard{
		ID:      cfg.ID,
		raft:    rn,
		engine:  engine,
		waiters: make(map[string]chan error),
	}, nil
}

// Start launches RaftNode and the apply consumer. The consumer reads the
// channel raft owns; it exits when raft closes that channel during Stop.
func (sh *Shard) Start() {
	sh.raft.Start()
	sh.wg.Add(1)
	go sh.consumeApply(sh.raft.ApplyCh())
}

// Stop tears down in the only safe order:
//  1. raft.Stop blocks until applyLoop drains (<-applyDone), then closes
//     applyCh. consumeApply must still be alive here to drain applyCh, or
//     applyLoop blocks on a send and raft.Stop deadlocks.
//  2. The applyCh close makes consumeApply finish draining and return.
//     wg.Wait joins it.
//  3. Engine closes last; nothing applies after this.
func (sh *Shard) Stop() error {
	sh.raft.Stop()
	sh.wg.Wait()
	if err := sh.engine.Close(); err != nil {
		return fmt.Errorf("shard %d engine close: %w", sh.ID, err)
	}
	return nil
}

// Propose marshals cmd, registers a waiter under cmd.RequestId, calls
// raft.Propose, and waits for the apply signal or the context deadline.
// Entry point for writes from API handlers.
func (sh *Shard) Propose(ctx context.Context, cmd *raftpb.Command) error {
	if cmd.RequestId == "" {
		return errors.New("shard.Propose: RequestId required")
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	waiter := sh.RegisterWaiter(cmd.RequestId)
	defer sh.DeleteWaiter(cmd.RequestId)

	if _, _, err := sh.raft.Propose(data); err != nil {
		return fmt.Errorf("raft propose: %w", err)
	}

	select {
	case applyErr := <-waiter:
		return applyErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sh *Shard) Get(key []byte) ([]byte, error) { return sh.engine.Get(key) }
func (sh *Shard) IsLeader() bool                 { return sh.raft.IsLeader() }
func (sh *Shard) LeaderHint() (string, bool)     { return sh.raft.LeaderHint() }

// RegisterWaiter / DeleteWaiter: exported for tests that drive the apply
// consumer directly with an injected channel. Production writes go through
// Propose.
func (sh *Shard) RegisterWaiter(requestID string) <-chan error {
	ch := make(chan error, 1)
	sh.waitersMu.Lock()
	sh.waiters[requestID] = ch
	sh.waitersMu.Unlock()
	return ch
}

func (sh *Shard) DeleteWaiter(requestID string) {
	sh.waitersMu.Lock()
	delete(sh.waiters, requestID)
	sh.waitersMu.Unlock()
}

func (sh *Shard) Status() raft.Status {
	return sh.raft.Status()
}

func (sh *Shard) StartRaftServer(addr string) error {
	return sh.raft.StartGRPCServer(addr)
}
