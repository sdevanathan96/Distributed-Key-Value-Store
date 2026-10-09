package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv/proto/kvpb"
)

// fakeNode is a KV server whose every call returns whatever reply says, and
// counts the calls it saw. reply can be swapped mid test.
type fakeNode struct {
	kvpb.UnimplementedKVServiceServer
	mu    sync.Mutex
	calls int
	reply func() error
	found bool
}

func (f *fakeNode) next() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.reply == nil {
		return nil
	}
	return f.reply()
}

func (f *fakeNode) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeNode) setReply(r func() error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reply = r
}

func (f *fakeNode) Put(context.Context, *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := f.next(); err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

func (f *fakeNode) Get(context.Context, *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if err := f.next(); err != nil {
		return nil, err
	}
	if !f.found {
		return &kvpb.GetResponse{Found: false}, nil // what server/api.go sends
	}
	return &kvpb.GetResponse{Found: true, Value: []byte("v")}, nil
}

// serve starts f on a free localhost port and returns its address and a stop
// function. Stop is idempotent and also runs at cleanup.
func serve(t *testing.T, f *fakeNode) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer()
	kvpb.RegisterKVServiceServer(g, f)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = g.Serve(lis)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			g.Stop()
			<-done
		})
	}
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

// deadAddr returns an address nothing listens on.
func deadAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func redirectTo(addr string) func() error {
	return func() error {
		st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&kvpb.LeaderHint{ClientAddr: addr})
		if err != nil {
			panic(err)
		}
		return st.Err()
	}
}

func noLeader() error { return status.Error(codes.Unavailable, "no leader elected") }

func fastOptions() Options {
	o := DefaultOptions()
	o.RetryBackoff = time.Millisecond
	return o
}

func newClient(t *testing.T, addrs []string, opts Options) *Client {
	t.Helper()
	c, err := New(addrs, opts)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	return c
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNewRejectsNoAddresses(t *testing.T) {
	_, err := New(nil, DefaultOptions())
	assert.Error(t, err)
}

func TestClientFollowsRedirectAndCachesLeader(t *testing.T) {
	leader := &fakeNode{}
	leaderAddr, _ := serve(t, leader)
	follower := &fakeNode{reply: redirectTo(leaderAddr)}
	followerAddr, _ := serve(t, follower)

	c := newClient(t, []string{followerAddr}, fastOptions())
	ctx := testCtx(t)

	require.NoError(t, c.Put(ctx, []byte("k"), []byte("v")))
	assert.Equal(t, leaderAddr, c.LastServed(), "the write should land on the hinted leader")
	assert.Equal(t, 1, follower.callCount())
	assert.Equal(t, 1, leader.callCount())

	require.NoError(t, c.Put(ctx, []byte("k"), []byte("v")))
	assert.Equal(t, 1, follower.callCount(), "a cached leader skips the follower")
	assert.Equal(t, 2, leader.callCount())
}

func TestClientNoFollowReturnsRedirect(t *testing.T) {
	leader := &fakeNode{}
	leaderAddr, _ := serve(t, leader)
	followerAddr, _ := serve(t, &fakeNode{reply: redirectTo(leaderAddr)})

	opts := fastOptions()
	opts.FollowRedirects = false
	c := newClient(t, []string{followerAddr}, opts)

	err := c.Put(testCtx(t), []byte("k"), []byte("v"))
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	hint, ok := leaderHint(st)
	assert.True(t, ok, "the redirect error should carry the hint")
	assert.Equal(t, leaderAddr, hint)
	assert.Zero(t, leader.callCount(), "no-follow must not contact the leader")
}

// Two nodes each naming the other as leader, as can happen mid election. The
// hop budget turns that into an error instead of a loop.
func TestClientRedirectLoopIsBounded(t *testing.T) {
	a, b := &fakeNode{}, &fakeNode{}
	aAddr, _ := serve(t, a)
	bAddr, _ := serve(t, b)
	a.setReply(redirectTo(bAddr))
	b.setReply(redirectTo(aAddr))

	opts := fastOptions()
	opts.MaxRedirects = 3
	c := newClient(t, []string{aAddr}, opts)

	err := c.Put(testCtx(t), []byte("k"), []byte("v"))
	assert.ErrorIs(t, err, ErrTooManyRedirects)
	assert.Equal(t, opts.MaxRedirects+1, a.callCount()+b.callCount(), "one initial attempt plus MaxRedirects hops")
}

func TestClientSkipsUnreachableSeed(t *testing.T) {
	live := &fakeNode{}
	liveAddr, _ := serve(t, live)

	c := newClient(t, []string{deadAddr(t), liveAddr}, fastOptions())
	require.NoError(t, c.Put(testCtx(t), []byte("k"), []byte("v")))
	assert.Equal(t, liveAddr, c.LastServed())
}

// Hand run step 4 in miniature: the cached leader dies, and the next call falls
// back to the seed list instead of failing.
func TestClientFallsBackWhenLeaderDies(t *testing.T) {
	first, second := &fakeNode{}, &fakeNode{}
	firstAddr, stopFirst := serve(t, first)
	secondAddr, _ := serve(t, second)

	c := newClient(t, []string{firstAddr, secondAddr}, fastOptions())
	ctx := testCtx(t)
	require.NoError(t, c.Put(ctx, []byte("k"), []byte("v")))
	require.Equal(t, firstAddr, c.LastServed())

	stopFirst()
	require.NoError(t, c.Put(ctx, []byte("k"), []byte("v")))
	assert.Equal(t, secondAddr, c.LastServed())
}

func TestClientGivesUpWithNoLeader(t *testing.T) {
	n := &fakeNode{reply: noLeader}
	addr, _ := serve(t, n)

	opts := fastOptions()
	opts.MaxRetries = 2
	c := newClient(t, []string{addr}, opts)

	err := c.Put(testCtx(t), []byte("k"), []byte("v"))
	assert.ErrorIs(t, err, ErrNoLeader)
	assert.Equal(t, opts.MaxRetries+1, n.callCount())
}

// A cluster mid election answers UNAVAILABLE until a leader appears; the
// client should ride that out within its retry budget.
func TestClientRetriesUntilLeaderElected(t *testing.T) {
	n := &fakeNode{}
	attempts := 0
	n.setReply(func() error { // called under n.mu
		attempts++
		if attempts < 3 {
			return noLeader()
		}
		return nil
	})
	addr, _ := serve(t, n)

	c := newClient(t, []string{addr}, fastOptions())
	require.NoError(t, c.Put(testCtx(t), []byte("k"), []byte("v")))
	assert.Equal(t, 3, n.callCount())
}

// DEADLINE_EXCEEDED is indeterminate (the write may still apply), so it must
// reach the caller untouched rather than be retried.
func TestClientDoesNotRetryDeadlineExceeded(t *testing.T) {
	n := &fakeNode{reply: func() error {
		return status.Error(codes.DeadlineExceeded, "write not committed within deadline")
	}}
	addr, _ := serve(t, n)

	c := newClient(t, []string{addr}, fastOptions())
	err := c.Put(testCtx(t), []byte("k"), []byte("v"))
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Equal(t, 1, n.callCount())
}

func TestClientGetMissingKeyIsNotAnError(t *testing.T) {
	addr, _ := serve(t, &fakeNode{found: false})
	c := newClient(t, []string{addr}, fastOptions())

	val, found, err := c.Get(testCtx(t), []byte("missing"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestClientUseAfterClose(t *testing.T) {
	addr, _ := serve(t, &fakeNode{})
	c, err := New([]string{addr}, fastOptions())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	assert.ErrorIs(t, c.Put(testCtx(t), []byte("k"), []byte("v")), ErrClosed)
}
