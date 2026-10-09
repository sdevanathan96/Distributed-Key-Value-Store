package sharding

import (
	"fmt"
	"sort"
	"strconv"
)

// DefaultVirtualNodes is the number of ring points per shard. More points even
// out the arc each shard owns. At 128, 100k keys land within 7% of the mean per
// shard at 3 or 4 shards and within 17% at 8, and the ring stays a few KB.
const DefaultVirtualNodes = 128

// HashName names the key hash. It is persisted with the layout (EnsureLayout)
// because changing the hash remaps keys exactly like changing the shard count.
const HashName = "fnv1a64-fmix64"

// HashRing is a consistent hash ring over shard IDs. Each shard owns
// virtualNodes points on a 64 bit ring, and a key belongs to the shard of the
// first point at or after the key's hash, wrapping at the top.
//
// A shard's point labels depend only on its own ID, never on the shard count.
// So a ring with shard N added keeps every existing point, and the only keys
// that change owner are the ones landing on N's new points (about 1/(N+1) of
// them), all of which move to N. Nothing moves between existing shards. That
// property is what a future resharding step would rely on; this codebase fixes
// the shard count at startup and does not move data.
//
// Every node must compute the same ring, and a restarted node must compute the
// ring it had before. The hash and the label format are therefore part of the
// on disk contract: changing either is a data migration, not a refactor.
type HashRing struct {
	points       []ringPoint // sorted by hash, then shard
	numShards    int
	virtualNodes int
}

type ringPoint struct {
	hash  uint64
	shard int
}

// NewHashRing builds the ring for shards 0..numShards-1.
func NewHashRing(numShards, virtualNodes int) (*HashRing, error) {
	if numShards < 1 {
		return nil, fmt.Errorf("hash ring: shard count must be at least 1, got %d", numShards)
	}
	if virtualNodes < 1 {
		return nil, fmt.Errorf("hash ring: virtual nodes must be at least 1, got %d", virtualNodes)
	}
	points := make([]ringPoint, 0, numShards*virtualNodes)
	for s := 0; s < numShards; s++ {
		for v := 0; v < virtualNodes; v++ {
			label := "shard-" + strconv.Itoa(s) + "-vnode-" + strconv.Itoa(v)
			points = append(points, ringPoint{hash: hash64([]byte(label)), shard: s})
		}
	}
	// The shard tiebreak keeps the order deterministic if two labels collide.
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		return points[i].shard < points[j].shard
	})
	return &HashRing{points: points, numShards: numShards, virtualNodes: virtualNodes}, nil
}

// ShardFor returns the shard owning key, always in [0, NumShards()).
func (r *HashRing) ShardFor(key []byte) int {
	h := hash64(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.points[i].shard
}

func (r *HashRing) NumShards() int { return r.numShards }

// Layout returns the parameters that decide key ownership, for EnsureLayout.
func (r *HashRing) Layout() Layout {
	return Layout{NumShards: r.numShards, VirtualNodes: r.virtualNodes, Hash: HashName}
}

// hash64 is FNV-1a 64 followed by the MurmurHash3 fmix64 finalizer.
//
// FNV-1a alone is not enough for a ring. Its final step multiplies by a prime,
// which carries a change in the last byte only toward the high bits, so keys
// that differ only at the end (user-1, user-2, ...) hash to nearly the same
// spot and pile onto one shard. fmix64 spreads every input bit across the word.
func hash64(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
