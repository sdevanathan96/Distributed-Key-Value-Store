package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-kv/internal/raft"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/sharding"
	"distributed-kv/proto/kvpb"
)

// freePort grabs an ephemeral port and releases it so the caller can bind it.
// There is a tiny race between release and rebind, acceptable for tests.
func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// eventually polls fn until it returns true or the timeout elapses. Copy your
// existing helper if you already have one in this package; do not duplicate.
func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func TestKVEndToEndSingleNode(t *testing.T) {
	raftAddr := freePort(t)
	clientAddr := freePort(t)

	cfg := shard.Config{
		ID:      0,
		DataDir: t.TempDir(),
		RaftConfig: raft.RaftConfig{
			NodeID:             "node-0",
			Peers:              map[string]string{},
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		},
	}
	sh, err := shard.NewShard(cfg)
	require.NoError(t, err)

	require.NoError(t, sh.StartRaftServer(raftAddr))

	srv := NewServer([]*shard.Shard{sh}, sharding.SingleShardRouter{})
	srv.Start()
	kv, err := NewKVServer(srv, clientAddr)
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- kv.Serve()
	}()

	t.Cleanup(func() {
		kv.Stop()
		select {
		case err := <-serveErr:
			assert.NoError(t, err, "kv.Serve")
		case <-time.After(2 * time.Second):
			t.Errorf("kv.Serve did not return within 2s of Stop")
		}
		if err := srv.Stop(); err != nil {
			t.Errorf("server stop: %v", err)
		}
	})

	eventually(t, 5*time.Second, func() bool {
		return sh.IsLeader()
	})

	conn, err := grpc.NewClient(
		clientAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := kvpb.NewKVServiceClient(conn)

	callCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 3*time.Second)
	}

	key := []byte("alpha")
	val := []byte("one")

	{
		ctx, cancel := callCtx()
		defer cancel()
		_, err := client.Put(ctx, &kvpb.PutRequest{Key: key, Value: val})
		require.NoError(t, err, "put should succeed on the leader")
	}

	{
		ctx, cancel := callCtx()
		defer cancel()
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		require.NoError(t, err)
		assert.True(t, resp.Found, "key should be found after put")
		assert.Equal(t, val, resp.Value)
	}

	{
		ctx, cancel := callCtx()
		defer cancel()
		_, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		require.NoError(t, err)
	}

	{
		ctx, cancel := callCtx()
		defer cancel()
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		require.NoError(t, err)
		assert.False(t, resp.Found, "key should not be found after delete")
	}
}
