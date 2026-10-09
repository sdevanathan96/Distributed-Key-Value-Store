package sharding

// Router maps a key to a shard ID. Contract: ShardFor always returns a value in
// [0, NumShards()), and the same key always maps to the same shard. The Server
// checks at construction that it holds exactly NumShards() shards ordered by
// ID, so a shard ID doubles as a slice index.
type Router interface {
	ShardFor(key []byte) int
	NumShards() int
}

// SingleShardRouter routes every key to shard 0.
type SingleShardRouter struct{}

func (SingleShardRouter) ShardFor(_ []byte) int { return 0 }
func (SingleShardRouter) NumShards() int        { return 1 }
