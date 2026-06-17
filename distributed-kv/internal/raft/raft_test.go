package raft

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func clusterConfig(t *testing.T, n int) ([]RaftConfig, []string) {
	t.Helper()
	addresses := make([]string, n)
	for i := 0; i < n; i++ {
		addresses[i] = freePort(t)
	}

	configs := make([]RaftConfig, n)
	for i := 0; i < n; i++ {
		peers := make(map[string]string)
		for j := 0; j < n; j++ {
			if i != j {
				peers[fmt.Sprintf("node-%d", j)] = addresses[j]
			}
		}
		configs[i] = RaftConfig{
			NodeID:             fmt.Sprintf("node-%d", i),
			Peers:              peers,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			DataDir:            t.TempDir(),
		}
	}
	return configs, addresses
}

func startCluster(t *testing.T, n int) ([]*RaftNode, []string) {
	t.Helper()
	configs, addresses := clusterConfig(t, n)
	nodes := make([]*RaftNode, n)

	for i := 0; i < n; i++ {
		node, err := NewRaftNode(configs[i])
		require.NoError(t, err)
		err = node.StartGRPCServer(addresses[i])
		require.NoError(t, err)
		nodes[i] = node
	}

	time.Sleep(100 * time.Millisecond)
	for _, node := range nodes {
		node.Start()
	}

	return nodes, addresses
}

func stopCluster(nodes []*RaftNode) {
	for _, node := range nodes {
		if node != nil {
			node.Stop()
		}
	}
}

func getLeader(nodes []*RaftNode) int {
	for i, node := range nodes {
		if node == nil {
			continue
		}
		node.mu.RLock()
		isLeader := node.state == Leader
		node.mu.RUnlock()
		if isLeader {
			return i
		}
	}
	return -1
}

func countState(nodes []*RaftNode, s NodeState) int {
	count := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}
		node.mu.RLock()
		if node.state == s {
			count++
		}
		node.mu.RUnlock()
	}
	return count
}

func waitForLeader(t *testing.T, nodes []*RaftNode, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx := getLeader(nodes)
		if idx >= 0 {
			return idx
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return -1
}

func getTerms(nodes []*RaftNode) []uint64 {
	terms := make([]uint64, len(nodes))
	for i, node := range nodes {
		node.mu.RLock()
		terms[i] = node.currentTerm
		node.mu.RUnlock()
	}
	return terms
}

func TestSingleNodeElection(t *testing.T) {
	config := RaftConfig{
		NodeID:             "node-0",
		Peers:              map[string]string{},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            t.TempDir(),
	}

	node, err := NewRaftNode(config)
	require.NoError(t, err)

	addr := freePort(t)
	require.NoError(t, node.StartGRPCServer(addr))
	node.Start()
	defer node.Stop()

	eventually(t, 2*time.Second, func() bool {
		node.mu.RLock()
		defer node.mu.RUnlock()
		return node.state == Leader
	})

	node.mu.RLock()
	defer node.mu.RUnlock()
	assert.Equal(t, uint64(1), node.currentTerm, "should be in term 1")
	assert.Equal(t, "node-0", node.leaderId)

}

func TestThreeNodeElection(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("Leader elected: node-%d", leaderIdx)

	assert.Equal(t, 1, countState(nodes, Leader), "should have exactly 1 leader")

	assert.Equal(t, 2, countState(nodes, Follower), "should have exactly 2 followers")

	terms := getTerms(nodes)
	assert.Equal(t, terms[0], terms[1], "all nodes should have same term")
	assert.Equal(t, terms[1], terms[2], "all nodes should have same term")
	t.Logf("All nodes at term %d", terms[0])
}

func TestFiveNodeElection(t *testing.T) {
	nodes, _ := startCluster(t, 5)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("Leader elected: node-%d", leaderIdx)

	assert.Equal(t, 1, countState(nodes, Leader), "should have exactly 1 leader")
	assert.Equal(t, 4, countState(nodes, Follower), "should have exactly 4 followers")

	terms := getTerms(nodes)
	for i := 1; i < len(terms); i++ {
		assert.Equal(t, terms[0], terms[i],
			"node-%d term %d should match node-0 term %d", i, terms[i], terms[0])
	}
}

func TestLeaderHeartbeatPreventsElection(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("Initial leader: node-%d", leaderIdx)

	nodes[leaderIdx].mu.RLock()
	initialTerm := nodes[leaderIdx].currentTerm
	nodes[leaderIdx].mu.RUnlock()

	consistently(t, 2*time.Second, func() bool {
		if getLeader(nodes) != leaderIdx {
			return false
		}
		nodes[leaderIdx].mu.RLock()
		defer nodes[leaderIdx].mu.RUnlock()
		return nodes[leaderIdx].currentTerm == initialTerm
	})
}

func TestReElectionAfterLeaderStop(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("Initial leader: node-%d", leaderIdx)

	nodes[leaderIdx].mu.RLock()
	oldTerm := nodes[leaderIdx].currentTerm
	nodes[leaderIdx].mu.RUnlock()

	nodes[leaderIdx].Stop()
	nodes[leaderIdx] = nil
	t.Logf("Stopped leader node-%d", leaderIdx)

	newLeaderIdx := waitForLeader(t, nodes, 3*time.Second)

	require.NotEqual(t, -1, newLeaderIdx, "a new leader should be elected")
	assert.NotEqual(t, leaderIdx, newLeaderIdx, "new leader should be different node")
	t.Logf("New leader: node-%d", newLeaderIdx)

	nodes[newLeaderIdx].mu.RLock()
	newTerm := nodes[newLeaderIdx].currentTerm
	nodes[newLeaderIdx].mu.RUnlock()
	assert.Greater(t, newTerm, oldTerm, "new term should be higher than old term")
	t.Logf("Term advanced from %d to %d", oldTerm, newTerm)
}

func TestReElectionFiveNodes(t *testing.T) {
	nodes, _ := startCluster(t, 5)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("Initial leader: node-%d", leaderIdx)

	nodes[leaderIdx].Stop()
	nodes[leaderIdx] = nil

	eventually(t, 2*time.Second, func() bool {
		return countState(nodes, Leader) == 1
	})
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)

	nodes[leaderIdx].mu.RLock()
	leaderID := nodes[leaderIdx].id
	nodes[leaderIdx].mu.RUnlock()

	for i, node := range nodes {
		if i == leaderIdx {
			continue
		}
		_, node := i, node
		eventually(t, 2*time.Second, func() bool {
			node.mu.RLock()
			defer node.mu.RUnlock()
			return node.leaderId == leaderID
		})
	}

}

func TestNoElectionWhileLeaderAlive(t *testing.T) {
	nodes, _ := startCluster(t, 5)
	defer stopCluster(nodes)

	waitForLeader(t, nodes, 3*time.Second)

	terms1 := getTerms(nodes)
	consistently(t, 3*time.Second, func() bool {
		terms2 := getTerms(nodes)
		for i := range nodes {
			if terms2[i] != terms1[i] {
				return false
			}
		}
		return true
	})
	assert.Equal(t, 1, countState(nodes, Leader))

}

func TestMultipleLeaderStops(t *testing.T) {
	nodes, _ := startCluster(t, 5)
	defer stopCluster(nodes)

	for kill := 0; kill < 2; kill++ {
		leaderIdx := waitForLeader(t, nodes, 3*time.Second)
		t.Logf("Kill #%d: stopping leader node-%d", kill+1, leaderIdx)
		nodes[leaderIdx].Stop()
		nodes[leaderIdx] = nil
		time.Sleep(1 * time.Second)
	}

	newLeaderCount := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}
		node.mu.RLock()
		if node.state == Leader {
			newLeaderCount++
		}
		node.mu.RUnlock()
	}

	assert.Equal(t, 1, newLeaderCount, "should still have a leader with 3 of 5 nodes")
}

func TestSingleNodeProposeApplies(t *testing.T) {
	cfg := RaftConfig{
		NodeID:             "node-0",
		Peers:              map[string]string{}, // single node: no peers
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            t.TempDir(),
	}
	rn, err := NewRaftNode(cfg)
	require.NoError(t, err)
	rn.Start()
	defer rn.Stop()

	eventually(t, 2*time.Second, func() bool { return rn.IsLeader() })

	cmd := []byte("hello")
	index, term, err := rn.Propose(cmd)
	require.NoError(t, err)

	select {
	case msg := <-rn.ApplyCh():
		assert.True(t, msg.CommandValid, "should be valid command")
		assert.Equal(t, cmd, msg.Command, "command should match proposed")
		assert.Equal(t, index, msg.CommandIndex, "command index should match proposed")
		assert.Equal(t, term, msg.CommandTerm, "command term should match proposed")
	case <-time.After(2 * time.Second):
		t.Fatal("proposed entry never applied: commit did not advance on a single node")
	}
	_ = term
}
