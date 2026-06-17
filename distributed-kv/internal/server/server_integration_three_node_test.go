package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"distributed-kv/internal/raft"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
	"distributed-kv/proto/kvpb"
)

// node bundles everything for one cluster member so the test can address them
// by index. Each node is its OWN Server wrapping its OWN single shard wrapping
// its OWN RaftNode; the three RaftNodes form one Raft group via Peers. This is
// the real topology: a 3 node cluster is 3 processes, not 1 Server with 3 shards.
type node struct {
	id         string
	raftAddr   string
	clientAddr string // = raft port + 1, MUST match RaftAddrToClientAddr
	sh         *shard.Shard
	srv        *Server
	kv         *KVServer
	serveErr   chan error
}

// freeRaftClientPairs returns n (raftAddr, clientAddr) pairs where clientAddr is
// raftPort+1 and all 2n ports are distinct and free. It holds listeners across
// the whole loop so the OS cannot hand the same port (or a neighbor) to two
// nodes, which is what made the naive freePort + "port+1" derivation collide.
func freeRaftClientPairs(t *testing.T, n int) (raftAddrs, clientAddrs []string) {
	t.Helper()
	var held []net.Listener
	defer func() {
		for _, l := range held {
			l.Close()
		}
	}()
	for len(raftAddrs) < n {
		lr, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		_, ps, err := net.SplitHostPort(lr.Addr().String())
		require.NoError(t, err)
		p, err := strconv.Atoi(ps)
		require.NoError(t, err)
		lc, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+1))
		if err != nil { // p+1 taken: keep lr held so we do not draw p again, retry
			held = append(held, lr)
			continue
		}
		held = append(held, lr, lc) // hold both so neither is reused by a later node
		raftAddrs = append(raftAddrs, fmt.Sprintf("127.0.0.1:%d", p))
		clientAddrs = append(clientAddrs, fmt.Sprintf("127.0.0.1:%d", p+1))
	}
	return
}

func TestKVEndToEndThreeNodeRedirect(t *testing.T) {
	const n = 3

	ids := make([]string, n)
	raftAddrs, clientAddrs := freeRaftClientPairs(t, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node-%d", i)
	}

	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		peers := map[string]string{}
		for j := 0; j < n; j++ {
			if i != j {
				peers[ids[j]] = raftAddrs[j]
			}
		}
		cfg := shard.Config{
			ID:      0,
			DataDir: t.TempDir(),
			RaftConfig: raft.RaftConfig{
				NodeID:             ids[i],
				Peers:              peers,
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
			},
		}
		sh, err := shard.NewShard(cfg)
		require.NoError(t, err)
		nodes[i] = &node{
			id:         ids[i],
			raftAddr:   raftAddrs[i],
			clientAddr: clientAddrs[i],
			sh:         sh,
			serveErr:   make(chan error, 1),
		}
	}

	for _, nd := range nodes {
		require.NoError(t, nd.sh.StartRaftServer(nd.raftAddr))
	}

	for _, nd := range nodes {
		nd.srv = NewServer([]*shard.Shard{nd.sh}, sharding.SingleShardRouter{})
		nd.srv.Start()

		kv, err := NewKVServer(nd.srv, nd.clientAddr)
		require.NoError(t, err)
		nd.kv = kv
		go func(nd *node) {
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

	eventually(t, 5*time.Second, func() bool {
		leaders := 0
		for _, nd := range nodes {
			if nd.sh.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	})

	var leader, follower *node
	for _, nd := range nodes {
		if nd.sh.IsLeader() {
			leader = nd
		} else if follower == nil {
			follower = nd
		}
	}
	require.NotNil(t, leader, "a leader should exist")
	require.NotNil(t, follower, "a follower should exist")

	eventually(t, 2*time.Second, func() bool {
		_, known := follower.sh.LeaderHint()
		return known
	})

	followerConn, err := grpc.NewClient(
		follower.clientAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = followerConn.Close() })
	followerClient := kvpb.NewKVServiceClient(followerConn)

	key := []byte("redirect-key")
	val := []byte("redirect-val")

	var hintAddr string
	{
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_, err := followerClient.Put(ctx, &kvpb.PutRequest{Key: key, Value: val})
		require.Error(t, err, "put on a follower must be rejected")

		st, ok := status.FromError(err)
		require.True(t, ok, "error should be a gRPC status")
		assert.Equal(t, codes.FailedPrecondition, st.Code(), "error code should be FailedPrecondition when hitting a follower")
		for _, d := range st.Details() {
			if h, ok := d.(*kvpb.LeaderHint); ok {
				hintAddr = h.ClientAddr
			}
		}
		assert.NotEmpty(t, hintAddr, "LeaderHint detail should be included in error")
		assert.Equal(t, leader.clientAddr, hintAddr, "LeaderHint should point to the leader's client addr")
	}

	{
		leaderConn, err := grpc.NewClient(
			hintAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer leaderConn.Close()
		leaderClient := kvpb.NewKVServiceClient(leaderConn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err = leaderClient.Put(ctx, &kvpb.PutRequest{Key: key, Value: val})
		require.NoError(t, err, "put on the hinted leader should succeed")

		gctx, gcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer gcancel()
		resp, err := leaderClient.Get(gctx, &kvpb.GetRequest{Key: key})
		require.NoError(t, err)
		assert.True(t, resp.Found, "key should be found after put")
		assert.Equal(t, val, resp.Value, "value should match what was put")

		dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer dcancel()
		_, err = leaderClient.Delete(dctx, &kvpb.DeleteRequest{Key: key})
		require.NoError(t, err, "delete on the hinted leader should succeed")

		gdctx, gdcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer gdcancel()
		resp, err = leaderClient.Get(gdctx, &kvpb.GetRequest{Key: key})
		require.NoError(t, err)
		assert.False(t, resp.Found, "key should not be found after delete")

	}
}
