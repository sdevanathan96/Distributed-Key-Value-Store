package raft

import (
	"context"
	"testing"
	"time"

	"distributed-kv/proto/raftpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newShardNode builds an unstarted node for shardID. Nothing runs, so the
// tests call handlers directly and inspect state with no goroutines to join.
func newShardNode(t *testing.T, shardID uint32) *RaftNode {
	t.Helper()
	rn, err := NewRaftNode(RaftConfig{
		NodeID:             "node-0",
		ShardID:            shardID,
		Peers:              map[string]string{},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { rn.electionTimer.Stop() })
	return rn
}

func termOf(rn *RaftNode) uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm
}

func logLen(rn *RaftNode) int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return len(rn.log)
}

func TestMuxDispatchesByShard(t *testing.T) {
	n0, n1 := newShardNode(t, 0), newShardNode(t, 1)
	mux, err := NewMux([]*RaftNode{n0, n1})
	require.NoError(t, err)
	ctx := context.Background()

	vote, err := mux.RequestVote(ctx, &raftpb.VoteRequest{ShardId: 1, Term: 5, CandidateId: "node-9"})
	require.NoError(t, err)
	assert.True(t, vote.VoteGranted, "shard 1 should grant the vote")
	assert.Equal(t, uint64(5), termOf(n1), "shard 1 adopts the candidate's term")
	assert.Equal(t, uint64(0), termOf(n0), "shard 0 must not see shard 1's election")

	ae, err := mux.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		ShardId:  0,
		Term:     3,
		LeaderId: "node-9",
		Entries:  []*raftpb.LogEntry{{Term: 3, Index: 1, Command: []byte("x")}},
	})
	require.NoError(t, err)
	assert.True(t, ae.Success)
	assert.Equal(t, 2, logLen(n0), "shard 0 appends the entry after its sentinel")
	assert.Equal(t, 1, logLen(n1), "shard 1's log must be untouched")
	assert.Equal(t, uint64(5), termOf(n1), "shard 1 keeps its own term")
}

func TestMuxRejectsUnknownShard(t *testing.T) {
	n0 := newShardNode(t, 0)
	mux, err := NewMux([]*RaftNode{n0})
	require.NoError(t, err)

	_, err = mux.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{
		ShardId: 7,
		Term:    9,
		Entries: []*raftpb.LogEntry{{Term: 9, Index: 1}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, uint64(0), termOf(n0), "a request for another shard must not change state")
	assert.Equal(t, 1, logLen(n0))

	_, err = mux.RequestVote(context.Background(), &raftpb.VoteRequest{ShardId: 7, Term: 9})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = mux.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotRequest{ShardId: 7})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestNewMuxRejectsDuplicateShard(t *testing.T) {
	_, err := NewMux([]*RaftNode{newShardNode(t, 2), newShardNode(t, 2)})
	assert.Error(t, err)
}

// The single node server (StartGRPCServer, used by the raft tests) has no mux
// in front of it, so the handler itself must refuse another shard's traffic.
func TestNodeServerRejectsOtherShard(t *testing.T) {
	n0 := newShardNode(t, 0)
	s := &raftServer{node: n0}

	_, err := s.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{
		ShardId: 1,
		Term:    4,
		Entries: []*raftpb.LogEntry{{Term: 4, Index: 1}},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = s.RequestVote(context.Background(), &raftpb.VoteRequest{ShardId: 1, Term: 4})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, uint64(0), termOf(n0))
	assert.Equal(t, 1, logLen(n0))
}
