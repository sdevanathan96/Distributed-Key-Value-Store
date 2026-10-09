package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv/proto/kvpb"
)

var (
	// ErrTooManyRedirects means the hop budget ran out. Almost always a real
	// election churning: node A thinks B leads, B thinks C does.
	ErrTooManyRedirects = errors.New("client: too many leader redirects")

	// ErrNoLeader means every attempt found no leader elected or an
	// unreachable node, within the retry budget.
	ErrNoLeader = errors.New("client: no leader available")
)

// outcome is how do() classifies one attempt.
type outcome int

const (
	outcomeOK       outcome = iota // success; remember this address
	outcomeRedirect                // FAILED_PRECONDITION + LeaderHint: go there
	outcomeRetry                   // UNAVAILABLE, or not leader with no hint: wait, try the next seed
	outcomeFatal                   // anything else: return it
)

// do runs fn against the best guess at the right node and handles the failure
// shapes the API returns (server/api.go, notLeader):
//
//	FAILED_PRECONDITION + LeaderHint -> leader known, go there
//	FAILED_PRECONDITION, no hint     -> not leader, leader unknown
//	UNAVAILABLE                      -> no leader elected yet
//
// gRPC also returns UNAVAILABLE when the node is down. The two UNAVAILABLEs
// cannot be told apart by code, and need not be: "back off, then ask the next
// seed" converges for both.
//
// DEADLINE_EXCEEDED is never retried. Writes are idempotent so a retry would be
// safe, but the outcome is indeterminate (the entry may still commit), and the
// Phase 2 linearizability checker has to see that indeterminacy to record the
// operation correctly. Swallowing it here would surface later as a fake Raft
// bug.
func (c *Client) do(ctx context.Context, fn func(context.Context, kvpb.KVServiceClient) error) error {
	target := c.startTarget()
	redirects, retries := 0, 0
	for {
		stub, err := c.conn(target)
		if err != nil {
			return err
		}
		actx, cancel := context.WithTimeout(ctx, c.opts.RequestTimeout)
		err = fn(actx, stub)
		cancel()

		hint, o := classify(err)
		switch o {
		case outcomeOK:
			c.mu.Lock()
			c.leader = target
			c.served = target
			c.mu.Unlock()
			return nil

		case outcomeRedirect:
			if !c.opts.FollowRedirects {
				return err
			}
			redirects++
			if redirects > c.opts.MaxRedirects {
				return fmt.Errorf("%w (last hint %s): %v", ErrTooManyRedirects, hint, err)
			}
			target = hint

		case outcomeRetry:
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			retries++
			if retries > c.opts.MaxRetries {
				return fmt.Errorf("%w: %v", ErrNoLeader, err)
			}
			c.forgetLeader(target)
			if err := sleep(ctx, c.opts.RetryBackoff); err != nil {
				return err
			}
			target = c.advanceSeed()

		default:
			return err
		}
	}
}

// startTarget is the cached leader if there is one, otherwise the current seed.
func (c *Client) startTarget() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.seeds[c.nextSeed]
}

// advanceSeed moves the round robin cursor and returns the new seed. After a
// failure the client falls back to the seed list, not the last hint: the hint
// is what just failed.
func (c *Client) advanceSeed() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextSeed = (c.nextSeed + 1) % len(c.seeds)
	return c.seeds[c.nextSeed]
}

// forgetLeader drops the cached leader if it is the address that just failed.
// Another goroutine may already have cached a newer one; leave that alone.
func (c *Client) forgetLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader == addr {
		c.leader = ""
	}
}

// classify maps one attempt's error onto an outcome, plus the redirect target
// when there is one.
func classify(err error) (addr string, o outcome) {
	if err == nil {
		return "", outcomeOK
	}
	st, ok := status.FromError(err)
	if !ok {
		return "", outcomeFatal
	}
	switch st.Code() {
	case codes.FailedPrecondition:
		if addr, ok := leaderHint(st); ok {
			return addr, outcomeRedirect
		}
		return "", outcomeRetry
	case codes.Unavailable:
		return "", outcomeRetry
	default:
		return "", outcomeFatal
	}
}

// leaderHint pulls the LeaderHint detail out of a status. Details() returns an
// error value in place of any entry that failed to unmarshal, so every element
// is type asserted. The address is already a client address (notLeader
// converts it); do not add 1 again.
func leaderHint(st *status.Status) (string, bool) {
	for _, d := range st.Details() {
		if h, ok := d.(*kvpb.LeaderHint); ok && h.GetClientAddr() != "" {
			return h.GetClientAddr(), true
		}
	}
	return "", false
}

// sleep waits d or until ctx is done, whichever is first.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
