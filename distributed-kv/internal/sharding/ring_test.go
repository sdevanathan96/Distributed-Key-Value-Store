package sharding

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func key(i int) []byte { return []byte(fmt.Sprintf("key-%d", i)) }

func TestHashRingRejectsBadConfig(t *testing.T) {
	_, err := NewHashRing(0, DefaultVirtualNodes)
	assert.Error(t, err, "zero shards")
	_, err = NewHashRing(-1, DefaultVirtualNodes)
	assert.Error(t, err, "negative shards")
	_, err = NewHashRing(4, 0)
	assert.Error(t, err, "zero virtual nodes")
}

func TestHashRingSingleShard(t *testing.T) {
	r, err := NewHashRing(1, DefaultVirtualNodes)
	require.NoError(t, err)
	assert.Equal(t, 1, r.NumShards())
	for i := 0; i < 1000; i++ {
		assert.Equal(t, 0, r.ShardFor(key(i)))
	}
	assert.Equal(t, 0, r.ShardFor(nil))
}

func TestHashRingShardForInRange(t *testing.T) {
	for _, n := range []int{2, 3, 7} {
		r, err := NewHashRing(n, DefaultVirtualNodes)
		require.NoError(t, err)
		for i := 0; i < 10000; i++ {
			s := r.ShardFor(key(i))
			if s < 0 || s >= n {
				t.Fatalf("n=%d: ShardFor(%q) = %d, out of range", n, key(i), s)
			}
		}
	}
}

// Two rings built independently must agree on every key. That is what lets
// every node route a key to the same Raft group without coordinating.
func TestHashRingDeterministic(t *testing.T) {
	a, err := NewHashRing(5, DefaultVirtualNodes)
	require.NoError(t, err)
	b, err := NewHashRing(5, DefaultVirtualNodes)
	require.NoError(t, err)
	for i := 0; i < 10000; i++ {
		if a.ShardFor(key(i)) != b.ShardFor(key(i)) {
			t.Fatalf("rings disagree on %q", key(i))
		}
	}
}

// Pins concrete assignments. If this fails, the hash or the point label format
// changed, which moves keys already stored on disk to groups that never stored
// them. That is a data migration: bump HashName so EnsureLayout refuses old
// data dirs, rather than editing these values to make the test pass.
func TestHashRingStableMapping(t *testing.T) {
	r, err := NewHashRing(4, DefaultVirtualNodes)
	require.NoError(t, err)
	cases := []struct {
		key   string
		shard int
	}{
		{"", 1},
		{"a", 2},
		{"alpha", 1},
		{"user-1", 0},
		{"user-2", 3},
		{"user-3", 1},
		{"order/42", 1},
		{"redirect-key", 3},
	}
	for _, c := range cases {
		assert.Equal(t, c.shard, r.ShardFor([]byte(c.key)), "key %q", c.key)
	}
}

func TestHashRingBalance(t *testing.T) {
	const keys = 100000
	for _, n := range []int{3, 4} {
		r, err := NewHashRing(n, DefaultVirtualNodes)
		require.NoError(t, err)
		counts := make([]int, n)
		for i := 0; i < keys; i++ {
			counts[r.ShardFor(key(i))]++
		}
		mean := float64(keys) / float64(n)
		for s, c := range counts {
			assert.InDelta(t, mean, float64(c), 0.10*mean, "n=%d shard %d holds %d keys (mean %.0f)", n, s, c, mean)
		}
	}
}

// Regression for the fmix64 finalizer. Plain FNV-1a sends user-1..user-9 to a
// single shard because keys that differ only in the last byte hash to nearly
// the same point.
func TestHashRingSpreadsSequentialKeys(t *testing.T) {
	r, err := NewHashRing(4, DefaultVirtualNodes)
	require.NoError(t, err)
	used := map[int]bool{}
	for i := 1; i <= 9; i++ {
		used[r.ShardFor([]byte(fmt.Sprintf("user-%d", i)))] = true
	}
	assert.GreaterOrEqual(t, len(used), 3, "user-1..user-9 should spread over most of 4 shards")
}

// The consistent hashing property: growing from 4 to 5 shards moves about a
// fifth of the keys, and every key that moves goes to the new shard. Nothing
// moves between shards that already existed.
func TestHashRingAddShardMovesKeysOnlyToNewShard(t *testing.T) {
	const keys = 100000
	before, err := NewHashRing(4, DefaultVirtualNodes)
	require.NoError(t, err)
	after, err := NewHashRing(5, DefaultVirtualNodes)
	require.NoError(t, err)

	moved := 0
	for i := 0; i < keys; i++ {
		from, to := before.ShardFor(key(i)), after.ShardFor(key(i))
		if from == to {
			continue
		}
		moved++
		if to != 4 {
			t.Fatalf("%q moved from shard %d to existing shard %d", key(i), from, to)
		}
	}
	frac := float64(moved) / keys
	assert.InDelta(t, 0.2, frac, 0.05, "fraction of keys moved")
}

func TestHashRingLayout(t *testing.T) {
	r, err := NewHashRing(3, 64)
	require.NoError(t, err)
	assert.Equal(t, Layout{NumShards: 3, VirtualNodes: 64, Hash: HashName}, r.Layout())
}
