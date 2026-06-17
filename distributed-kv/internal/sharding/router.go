package sharding

// Router maps a key to a shard ID. The MVP implementation is single shard
// (always returns 0). A consistent hash ring will implement this same
// interface when multi shard lands, so the Server does not change.
type Router interface {
	ShardFor(key []byte) int
	NumShards() int
}

// SingleShardRouter routes every key to shard 0.
type SingleShardRouter struct{}

func (SingleShardRouter) ShardFor(_ []byte) int { return 0 }
func (SingleShardRouter) NumShards() int        { return 1 }
