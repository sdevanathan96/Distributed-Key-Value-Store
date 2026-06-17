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

	s := NewServer([]*shard.Shard{sh0, sh1}, router)
	assert.Same(t, sh0, s.ShardFor([]byte("ab")))
	assert.Same(t, sh1, s.ShardFor([]byte("abc")))
}

func TestServerRoutingSingleShard(t *testing.T) {
	sh0 := mustNewShard(t, 0)
	s := NewServer([]*shard.Shard{sh0}, sharding.SingleShardRouter{})
	assert.Same(t, sh0, s.ShardFor([]byte("anything")))
	assert.Same(t, sh0, s.ShardFor([]byte("")))
}
