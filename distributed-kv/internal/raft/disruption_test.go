package raft

import (
	"context"
	"testing"
	"time"

	"distributed-kv/proto/raftpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reproduces hand run step 5. A follower that was down long enough for its
// peers' gRPC reconnect backoff to grow comes back on the same address. The
// leader cannot reach it yet, so it times out again and again; each timeout
// used to bump its term and depose the healthy leader (terms went 3 to 40).
func TestRestartedFollowerDoesNotDisruptLeader(t *testing.T) {
	configs, addresses := clusterConfig(t, 3)
	nodes := startFromConfigs(t, configs, addresses)
	defer func() { stopCluster(nodes) }()

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	leader := nodes[leaderIdx]
	followerIdx := (leaderIdx + 1) % 3
	nodes[followerIdx].Stop()
	nodes[followerIdx] = nil

	// Commit while it is down so its log is behind.
	idx, _, err := leader.Propose([]byte("while-down"))
	require.NoError(t, err)
	// Not a wait on a condition: the downtime itself is the setup. Three
	// seconds lets the default reconnect backoff grow past an election timeout.
	time.Sleep(3 * time.Second)
	require.True(t, leader.IsLeader(), "leader should hold with one follower down")
	leaderTerm := termOf(leader)

	rejoined, err := NewRaftNode(configs[followerIdx])
	require.NoError(t, err)
	require.NoError(t, rejoined.StartGRPCServer(addresses[followerIdx]))
	rejoined.Start()
	nodes[followerIdx] = rejoined

	consistently(t, 2*time.Second, func() bool {
		return leader.IsLeader() && termOf(leader) == leaderTerm
	})
	waitForCommit(t, rejoined, idx, 3*time.Second)
	assert.Equal(t, leaderTerm, termOf(rejoined), "the rejoined node should adopt the cluster's term, not inflate it")
}

// follower returns an unstarted node at term 3 that last heard from leader
// node-1 the given time ago.
func follower(t *testing.T, leaderContactAgo time.Duration) *RaftNode {
	t.Helper()
	rn := newShardNode(t, 0)
	rn.currentTerm = 3
	rn.leaderId = "node-1"
	rn.lastLeaderContact = time.Now().Add(-leaderContactAgo)
	return rn
}

func TestPreVoteChangesNoState(t *testing.T) {
	rn := follower(t, time.Second)
	resp, err := (&raftServer{node: rn}).RequestVote(context.Background(),
		&raftpb.VoteRequest{Term: 4, CandidateId: "node-2", PreVote: true})
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted, "leader silent and log up to date")
	assert.Equal(t, uint64(3), termOf(rn), "a pre-vote must not move the term")
	assert.Empty(t, rn.votedFor, "a pre-vote must not record a vote")
}

func TestPreVoteRefusedWhileLeaderLive(t *testing.T) {
	rn := follower(t, 0)
	resp, err := (&raftServer{node: rn}).RequestVote(context.Background(),
		&raftpb.VoteRequest{Term: 4, CandidateId: "node-2", PreVote: true})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)
}

func TestPreVoteRefusedForStaleLog(t *testing.T) {
	rn := follower(t, time.Second)
	rn.log = append(rn.log, LogEntry{Term: 3, Index: 1})
	resp, err := (&raftServer{node: rn}).RequestVote(context.Background(),
		&raftpb.VoteRequest{Term: 4, CandidateId: "node-2", LastLogIndex: 0, LastLogTerm: 0, PreVote: true})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted, "a candidate missing entries cannot win, so it should not even try")
}

// The core of the fix: a higher term vote request from a node that cannot hear
// the leader must not move this node's term or forget its leader.
func TestHigherTermVoteIgnoredWhileLeaderLive(t *testing.T) {
	rn := follower(t, 0)
	resp, err := (&raftServer{node: rn}).RequestVote(context.Background(),
		&raftpb.VoteRequest{Term: 9, CandidateId: "node-2"})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	assert.Equal(t, uint64(3), termOf(rn))
	assert.Equal(t, "node-1", rn.leaderId)
}

func TestHigherTermVoteGrantedOnceLeaderSilent(t *testing.T) {
	rn := follower(t, time.Second)
	resp, err := (&raftServer{node: rn}).RequestVote(context.Background(),
		&raftpb.VoteRequest{Term: 4, CandidateId: "node-2"})
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(4), termOf(rn))
	assert.Equal(t, "node-2", rn.votedFor)
}

// CheckQuorum: a leader cut off from every follower stops acting as leader, so
// it stops serving leader only reads. PreVote then keeps the isolated node
// from inflating its term while it keeps timing out.
func TestLeaderStepsDownWithoutQuorum(t *testing.T) {
	configs, addresses := clusterConfig(t, 3)
	nodes := startFromConfigs(t, configs, addresses)
	defer func() { stopCluster(nodes) }()

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	leader := nodes[leaderIdx]
	term := termOf(leader)
	for i := range nodes {
		if i != leaderIdx {
			nodes[i].Stop()
			nodes[i] = nil
		}
	}

	eventually(t, 2*time.Second, func() bool { return !leader.IsLeader() })
	leader.mu.RLock()
	assert.Equal(t, configs[leaderIdx].NodeID, leader.votedFor, "stepping down must keep this term's vote")
	leader.mu.RUnlock()
	consistently(t, time.Second, func() bool { return termOf(leader) == term })
}
