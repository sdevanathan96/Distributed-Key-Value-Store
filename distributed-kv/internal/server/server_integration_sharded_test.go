package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"distributed-kv/internal/client"
	"distributed-kv/internal/raft"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
	"distributed-kv/internal/storage"
)

// shardedNode is one process in the sharded topology: one Server hosting every
// shard, all of them behind one Raft port.
type shardedNode struct {
	id         string
	clientAddr string
	shards     []*shard.Shard
	srv        *Server
	kv         *KVServer
	serveErr   chan error
}

// TestKVEndToEndThreeNodeSharded runs 3 nodes x 3 shards, which is three
// independent Raft groups sharing three Raft ports through raft.Mux. Traffic
// goes through internal/client seeded with a single node, so any shard led
// elsewhere forces a redirect. Placement is then checked on every node's
// engines directly: each key lives in the shard the ring assigns it, on every
// replica, and in no other shard.
func TestKVEndToEndThreeNodeSharded(t *testing.T) {
	const nodeCount, shardCount = 3, 3
	ring, err := sharding.NewHashRing(shardCount, sharding.DefaultVirtualNodes)
	require.NoError(t, err)

	ids := make([]string, nodeCount)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	raftAddrs, clientAddrs := freeRaftClientPairs(t, nodeCount)

	nodes := make([]*shardedNode, nodeCount)
	for i := range nodes {
		peers := map[string]string{}
		for j := range ids {
			if i != j {
				peers[ids[j]] = raftAddrs[j]
			}
		}
		dataDir := t.TempDir()
		shards := make([]*shard.Shard, shardCount)
		for s := range shards {
			sh, err := shard.NewShard(shard.Config{
				ID:      s,
				DataDir: filepath.Join(dataDir, fmt.Sprintf("shard-%d", s)),
				RaftConfig: raft.RaftConfig{
					NodeID:             ids[i],
					Peers:              peers,
					ElectionTimeoutMin: 150 * time.Millisecond,
					ElectionTimeoutMax: 300 * time.Millisecond,
					HeartbeatInterval:  50 * time.Millisecond,
				},
			})
			require.NoError(t, err)
			shards[s] = sh
		}
		srv, err := NewServer(shards, ring)
		require.NoError(t, err)
		require.NoError(t, srv.StartRaftServer(raftAddrs[i]))
		nodes[i] = &shardedNode{
			id:         ids[i],
			clientAddr: clientAddrs[i],
			shards:     shards,
			srv:        srv,
			serveErr:   make(chan error, 1),
		}
	}

	for _, nd := range nodes {
		nd.srv.Start()
		kv, err := NewKVServer(nd.srv, nd.clientAddr)
		require.NoError(t, err)
		nd.kv = kv
		go func(nd *shardedNode) {
			nd.serveErr <- nd.kv.Serve()
		}(nd)
	}

	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.kv.Stop()
			select {
			case err := <-nd.serveErr:
				assert.NoError(t, err, "kv.Serve %s", nd.id)
			case <-time.After(2 * time.Second):
				t.Errorf("kv.Serve %s did not return within 2s of Stop", nd.id)
			}
			if err := nd.srv.Stop(); err != nil {
				t.Errorf("server stop %s: %v", nd.id, err)
			}
		}
	})

	// Every shard elects its own leader.
	eventually(t, 5*time.Second, func() bool {
		for s := 0; s < shardCount; s++ {
			leaders := 0
			for _, nd := range nodes {
				if nd.shards[s].IsLeader() {
					leaders++
				}
			}
			if leaders != 1 {
				return false
			}
		}
		return true
	})
	for s := 0; s < shardCount; s++ {
		for _, nd := range nodes {
			if nd.shards[s].IsLeader() {
				t.Logf("shard %d leader: %s", s, nd.id)
			}
		}
	}

	// Registered after the server cleanup, so it runs first (LIFO) and the
	// client's connections are closed before the servers stop.
	c, err := client.New([]string{nodes[0].clientAddr}, client.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, c.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const keyCount = 30
	keyAt := func(i int) []byte { return []byte(fmt.Sprintf("k-%d", i)) }
	valAt := func(i int) string { return fmt.Sprintf("v-%d", i) }

	owned := make([]int, shardCount)
	for i := 0; i < keyCount; i++ {
		require.NoError(t, c.Put(ctx, keyAt(i), []byte(valAt(i))), "put %s", keyAt(i))
		owned[ring.ShardFor(keyAt(i))]++
	}
	for s, n := range owned {
		require.NotZero(t, n, "shard %d owns none of the test keys, so the test would not exercise it", s)
	}

	for i := 0; i < keyCount; i++ {
		val, found, err := c.Get(ctx, keyAt(i))
		require.NoError(t, err, "get %s", keyAt(i))
		assert.True(t, found, "get %s", keyAt(i))
		assert.Equal(t, valAt(i), string(val), "get %s", keyAt(i))
	}

	// Followers apply on the heartbeat after commit, hence eventually for the
	// owning shard. The absence check needs no wait: a key is only ever
	// proposed to its owner, so it can never appear in another shard.
	for i := 0; i < keyCount; i++ {
		k, want, owner := keyAt(i), valAt(i), ring.ShardFor(keyAt(i))
		for _, nd := range nodes {
			eventually(t, 2*time.Second, func() bool {
				got, err := nd.shards[owner].Get(k)
				return err == nil && string(got) == want
			})
			for s := 0; s < shardCount; s++ {
				if s == owner {
					continue
				}
				_, err := nd.shards[s].Get(k)
				assert.ErrorIs(t, err, storage.ErrKeyNotFound, "%s: key %s found in shard %d, owner is %d", nd.id, k, s, owner)
			}
		}
	}

	for i := 0; i < keyCount; i += 2 {
		require.NoError(t, c.Delete(ctx, keyAt(i)), "delete %s", keyAt(i))
		_, found, err := c.Get(ctx, keyAt(i))
		require.NoError(t, err)
		assert.False(t, found, "%s should be gone after delete", keyAt(i))
	}

	// ClusterStatus from each node lists every shard, and all nodes agree on
	// each shard's leader.
	perNode := make([]*client.Client, nodeCount)
	for i, nd := range nodes {
		pc, err := client.New([]string{nd.clientAddr}, client.DefaultOptions())
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, pc.Close()) })
		perNode[i] = pc
	}
	eventually(t, 2*time.Second, func() bool {
		var leaders []string
		for i, pc := range perNode {
			st, err := pc.ClusterStatus(ctx)
			if err != nil || st.GetNodeId() != nodes[i].id || len(st.GetShards()) != shardCount {
				return false
			}
			for s, sh := range st.GetShards() {
				if int(sh.GetShardId()) != s || sh.GetLeaderId() == "" || len(sh.GetNodes()) != nodeCount {
					return false
				}
				if i == 0 {
					leaders = append(leaders, sh.GetLeaderId())
				} else if leaders[s] != sh.GetLeaderId() {
					return false
				}
			}
		}
		return true
	})
}
