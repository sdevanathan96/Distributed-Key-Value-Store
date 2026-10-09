package server

import (
	"distributed-kv/internal/raft"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRouter lets the test control routing so it can assert that ShardFor
// indexes into the slice by whatever the router returns. SingleShardRouter
// always returns 0 and would never catch an indexing bug, which is the whole
// thing this test exists to catch.
type fakeRouter struct {
	numShards int
	fn        func(key []byte) int
}

func (r fakeRouter) ShardFor(key []byte) int { return r.fn(key) }
func (r fakeRouter) NumShards() int          { return r.numShards }

// mustNewShard builds a real but unstarted shard. NewShard constructs Raft and
// Engine but does not launch goroutines, so there is nothing to tear down and
// no goleak surface. id distinguishes the shards for identity assertions.
func mustNewShard(t *testing.T, id int) *shard.Shard {
	t.Helper()
	cfg := shard.Config{
		ID:      id,
		DataDir: t.TempDir(),
		RaftConfig: raft.RaftConfig{
			NodeID:             fmt.Sprintf("node-%d", id),
			Peers:              map[string]string{},
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			// DataDir is overwritten inside NewShard to DataDir/raft; this
			// field can be left zero here since NewShard sets it.
		},
	}
	sh, err := shard.NewShard(cfg)
	require.NoError(t, err)
	return sh
}

func TestServerRouting(t *testing.T) {
	sh0 := mustNewShard(t, 0)
	sh1 := mustNewShard(t, 1)

	router := fakeRouter{
		numShards: 2,
		fn:        func(k []byte) int { return len(k) % 2 },
	}

	s, err := NewServer([]*shard.Shard{sh0, sh1}, router)
	require.NoError(t, err)
	assert.Same(t, sh0, s.ShardFor([]byte("ab")))
	assert.Same(t, sh1, s.ShardFor([]byte("abc")))
}

func TestServerRoutingSingleShard(t *testing.T) {
	sh0 := mustNewShard(t, 0)
	s, err := NewServer([]*shard.Shard{sh0}, sharding.SingleShardRouter{})
	require.NoError(t, err)
	assert.Same(t, sh0, s.ShardFor([]byte("anything")))
	assert.Same(t, sh0, s.ShardFor([]byte("")))
}

func TestNewServerRejectsNoShards(t *testing.T) {
	_, err := NewServer(nil, sharding.SingleShardRouter{})
	assert.Error(t, err)
}

// A router that can return an index past the end of the slice would panic
// inside a gRPC handler and kill the node, so the count must match up front.
func TestNewServerRejectsShardCountMismatch(t *testing.T) {
	sh0 := mustNewShard(t, 0)
	router := fakeRouter{numShards: 2, fn: func([]byte) int { return 1 }}
	_, err := NewServer([]*shard.Shard{sh0}, router)
	assert.Error(t, err)
}

// ShardFor uses the router's shard ID as a slice index. Shards passed out of
// ID order would route keys into the wrong Raft group with no error at all.
func TestNewServerRejectsShardsOutOfOrder(t *testing.T) {
	sh0 := mustNewShard(t, 0)
	sh1 := mustNewShard(t, 1)
	router := fakeRouter{numShards: 2, fn: func(k []byte) int { return len(k) % 2 }}
	_, err := NewServer([]*shard.Shard{sh1, sh0}, router)
	assert.Error(t, err)
}
