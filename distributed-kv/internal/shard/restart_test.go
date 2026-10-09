package shard

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"distributed-kv/internal/raft"
	"distributed-kv/internal/storage"
	"distributed-kv/proto/raftpb"
)

func singleNodeConfig(dataDir string) Config {
	return Config{
		ID:      0,
		DataDir: dataDir,
		RaftConfig: raft.RaftConfig{
			NodeID:             "node-0",
			Peers:              map[string]string{},
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		},
	}
}

// runSession starts a single node shard on dataDir, waits for it to lead,
// applies puts in order, and stops it.
func runSession(t *testing.T, dataDir string, puts ...[2]string) {
	t.Helper()
	sh, err := NewShard(singleNodeConfig(dataDir))
	require.NoError(t, err)
	sh.Start()
	defer func() { require.NoError(t, sh.Stop()) }()

	deadline := time.Now().Add(3 * time.Second)
	for !sh.IsLeader() {
		require.True(t, time.Now().Before(deadline), "no leader within 3s")
		time.Sleep(10 * time.Millisecond)
	}
	for i, kv := range puts {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := sh.Propose(ctx, &raftpb.Command{
			RequestId: fmt.Sprintf("%s-%d", t.Name(), i),
			Op:        raftpb.Op_OP_PUT,
			Key:       []byte(kv[0]),
			Value:     []byte(kv[1]),
		})
		cancel()
		require.NoError(t, err, "put %s", kv[0])
	}
}

// Each restart used to replay the whole Raft log through the applier, which
// wrote every old command into the WAL again (4 writes then 1 left 9 records)
// and briefly exposed overwritten values. Now the WAL holds each write once.
func TestShardRestartDoesNotReapply(t *testing.T) {
	dataDir := t.TempDir()
	runSession(t, dataDir, [2]string{"a", "1"}, [2]string{"b", "1"}, [2]string{"a", "2"})
	runSession(t, dataDir, [2]string{"c", "1"})

	wal, err := storage.NewWAL(storage.DefaultConfig(filepath.Join(dataDir, "storage")))
	require.NoError(t, err)
	defer wal.Close()
	entries, err := wal.Recover()
	require.NoError(t, err)
	seqs := make([]uint64, len(entries))
	for i, e := range entries {
		seqs[i] = e.Seq
	}
	assert.Equal(t, []uint64{1, 2, 3, 4}, seqs, "each log index should be applied exactly once")

	// Opened but never started, so close the engine directly: raft.Stop waits
	// for an apply loop that only Start launches.
	sh, err := NewShard(singleNodeConfig(dataDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, sh.engine.Close()) }()
	assert.Equal(t, uint64(4), sh.engine.AppliedIndex())
	val, err := sh.Get([]byte("a"))
	require.NoError(t, err)
	assert.Equal(t, "2", string(val))
}
