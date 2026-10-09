// Package client is the KV client library: dial a node, issue Put/Get/Delete,
// and follow the leader redirect the server hands back when it is not leader.
//
// Two consumers: cmd/client (kvctl), and the Phase 2 Porcupine harness. The
// harness is why this is a package and not logic inside main.go: it exercises
// the same redirect path that ships, and a call's start to return interval
// spans the retries, which is the in flight window linearizability checks.
//
// The client knows nothing about shards. Every node routes a key to its shard,
// and a node that is not that shard's leader redirects to the one that is. The
// client caches only the last address that served a call, so with several
// shards a request costs at most one extra hop when its shard's leader lives
// on a different node.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-kv/proto/kvpb"
)

// Options tunes retry behavior. Start from DefaultOptions.
type Options struct {
	// FollowRedirects: on FAILED_PRECONDITION + LeaderHint, retry against the
	// hinted address. Off means the redirect error is returned as is, which is
	// how a caller asserts that a follower rejects a write.
	FollowRedirects bool

	// MaxRedirects bounds hops per call. Mid election two nodes can hold stale
	// leader ids pointing at each other; without a bound that is a loop.
	MaxRedirects int

	// MaxRetries bounds attempts after UNAVAILABLE (no leader elected, or the
	// node is down). Each retry moves to the next seed after RetryBackoff.
	// Redirects and retries have separate budgets: a redirect is progress, a
	// retry is waiting, and one should not starve the other.
	MaxRetries int

	// RequestTimeout is the per attempt deadline. Keep it above the server's 5s
	// propose timeout (server/api.go): the server derives its propose context
	// from the request context, so a shorter client deadline cancels the write
	// mid propose and you get a bare client timeout instead of the server's
	// "write not committed within deadline".
	RequestTimeout time.Duration

	// RetryBackoff is the pause before a retry. An election takes 150 to 300ms.
	RetryBackoff time.Duration
}

func DefaultOptions() Options {
	return Options{
		FollowRedirects: true,
		MaxRedirects:    4,
		MaxRetries:      15,
		RequestTimeout:  6 * time.Second,
		RetryBackoff:    200 * time.Millisecond,
	}
}

// ErrClosed is returned by any call made after Close.
var ErrClosed = errors.New("client: closed")

// Client is safe for concurrent use; the Phase 2 harness shares one across
// goroutines.
type Client struct {
	opts  Options
	seeds []string // client addresses (raft port + 1) given to New

	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn // nil after Close
	leader   string                      // last address that served a call, "" if none
	served   string                      // address that served the most recent call
	nextSeed int                         // round robin cursor into seeds
}

// New takes the client addresses of any nodes you know about. Dialing is lazy,
// so a down node here is not an error; it surfaces as UNAVAILABLE on use and
// the client moves on to the next address.
func New(addrs []string, opts Options) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: at least one address required")
	}
	return &Client{
		opts:  opts,
		seeds: append([]string(nil), addrs...),
		conns: make(map[string]*grpc.ClientConn),
	}, nil
}

// Close closes every connection the client opened.
func (c *Client) Close() error {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()

	var errs []error
	for addr, conn := range conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", addr, err))
		}
	}
	return errors.Join(errs...)
}

// LastServed reports the address that served the most recent successful call,
// so kvctl can show where a redirected write actually landed.
func (c *Client) LastServed() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.served
}

// Get returns found=false with a nil error for a missing key, mirroring the
// server, so "absent" and "the call failed" stay distinguishable.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	var resp *kvpb.GetResponse
	err = c.do(ctx, func(ctx context.Context, kv kvpb.KVServiceClient) error {
		r, err := kv.Get(ctx, &kvpb.GetRequest{Key: key})
		resp = r
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.do(ctx, func(ctx context.Context, kv kvpb.KVServiceClient) error {
		_, err := kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		return err
	})
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.do(ctx, func(ctx context.Context, kv kvpb.KVServiceClient) error {
		_, err := kv.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		return err
	})
}

// ClusterStatus is answered by any node, so it never redirects; it still
// retries the next seed if the first one is down.
func (c *Client) ClusterStatus(ctx context.Context) (*kvpb.ClusterStatusResponse, error) {
	var resp *kvpb.ClusterStatusResponse
	err := c.do(ctx, func(ctx context.Context, kv kvpb.KVServiceClient) error {
		r, err := kv.ClusterStatus(ctx, &kvpb.ClusterStatusRequest{})
		resp = r
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// conn returns a stub for addr, dialing on first use. grpc.NewClient does not
// connect eagerly, so this fails only on a malformed target or after Close.
func (c *Client) conn(addr string) (kvpb.KVServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conns == nil {
		return nil, ErrClosed
	}
	if conn, ok := c.conns[addr]; ok {
		return kvpb.NewKVServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return kvpb.NewKVServiceClient(conn), nil
}
