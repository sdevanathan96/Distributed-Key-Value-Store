package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getLog(node *RaftNode) []LogEntry {
	node.mu.RLock()
	defer node.mu.RUnlock()
	cp := make([]LogEntry, len(node.log))
	copy(cp, node.log)
	return cp
}

func waitForCommit(t *testing.T, node *RaftNode, index uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		ci := node.commitIndex
		node.mu.RUnlock()
		if ci >= index {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("commitIndex did not reach %d within %v", index, timeout)
}

func drainUntil(ch chan ApplyMsg, index uint64, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return false
			}
			if msg.CommandValid && msg.CommandIndex == index {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

func startFromConfigs(t *testing.T, configs []RaftConfig, addresses []string) []*RaftNode {
	t.Helper()
	nodes := make([]*RaftNode, len(configs))
	for i := range configs {
		node, err := NewRaftNode(configs[i])
		require.NoError(t, err)
		require.NoError(t, node.StartGRPCServer(addresses[i]))
		nodes[i] = node
	}
	time.Sleep(100 * time.Millisecond)
	for _, node := range nodes {
		node.Start()
	}
	return nodes
}

func TestProposeOnLeader(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	idx, term, err := nodes[leaderIdx].Propose([]byte("cmd-1"))
	require.NoError(t, err)
	assert.Greater(t, idx, uint64(0))
	assert.Greater(t, term, uint64(0))

	nodes[leaderIdx].mu.RLock()
	lastEntry := nodes[leaderIdx].log[len(nodes[leaderIdx].log)-1]
	nodes[leaderIdx].mu.RUnlock()
	assert.Equal(t, idx, lastEntry.Index)
}

func TestProposeOnFollowerFails(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	followerIdx := (leaderIdx + 1) % 3

	_, _, err := nodes[followerIdx].Propose([]byte("cmd"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not leader")
}

func TestLogReplication(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)

	var lastIdx uint64
	for i := 0; i < 5; i++ {
		idx, _, err := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
		lastIdx = idx
	}

	for _, node := range nodes {
		waitForCommit(t, node, lastIdx, 2*time.Second)
	}

	leaderLog := getLog(nodes[leaderIdx])
	for i, node := range nodes {
		assert.Equal(t, leaderLog, getLog(node), "node-%d log mismatch", i)
	}
}

func TestApplyAfterCommit(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	idx, _, err := nodes[leaderIdx].Propose([]byte("x=1"))
	require.NoError(t, err)

	for i, node := range nodes {
		assert.True(t,
			drainUntil(node.applyCh, idx, 2*time.Second),
			"node-%d did not apply index %d", i, idx)
	}
}

func TestLogConsistencyAfterPartition(t *testing.T) {
	configs, addresses := clusterConfig(t, 5)
	nodes := startFromConfigs(t, configs, addresses)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)

	for i := 0; i < 3; i++ {
		_, _, err := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("pre-%d", i)))
		require.NoError(t, err)
	}
	time.Sleep(300 * time.Millisecond)

	var stoppedIdxs []int
	for i := range nodes {
		if i != leaderIdx && len(stoppedIdxs) < 2 {
			nodes[i].Stop()
			nodes[i] = nil
			stoppedIdxs = append(stoppedIdxs, i)
		}
	}

	var lastIdx uint64
	for i := 0; i < 3; i++ {
		idx, _, err := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("post-%d", i)))
		require.NoError(t, err)
		lastIdx = idx
	}
	time.Sleep(300 * time.Millisecond)

	for _, i := range stoppedIdxs {
		node, err := NewRaftNode(configs[i])
		require.NoError(t, err)
		require.NoError(t, node.StartGRPCServer(addresses[i]))
		node.Start()
		nodes[i] = node
	}

	for _, i := range stoppedIdxs {
		waitForCommit(t, nodes[i], lastIdx, 3*time.Second)
	}

	leaderLog := getLog(nodes[leaderIdx])
	for i, node := range nodes {
		assert.Equal(t, leaderLog, getLog(node), "node-%d log mismatch after partition heal", i)
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	configs, addresses := clusterConfig(t, 3)
	nodes := startFromConfigs(t, configs, addresses)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)

	var lastIdx uint64
	for i := 0; i < 5; i++ {
		idx, _, err := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
		lastIdx = idx
	}
	for _, node := range nodes {
		waitForCommit(t, node, lastIdx, 2*time.Second)
	}

	savedLog := getLog(nodes[leaderIdx])
	stopCluster(nodes)
	time.Sleep(200 * time.Millisecond)

	nodes = startFromConfigs(t, configs, addresses)
	defer stopCluster(nodes)

	newLeaderIdx := waitForLeader(t, nodes, 3*time.Second)

	for i, node := range nodes {
		assert.Equal(t, savedLog, getLog(node), "node-%d log not restored after restart", i)
	}

	_, _, err := nodes[newLeaderIdx].Propose([]byte("post-restart"))
	require.NoError(t, err)
}

func TestFollowerCatchUp(t *testing.T) {
	configs, addresses := clusterConfig(t, 3)
	nodes := startFromConfigs(t, configs, addresses)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)
	slowIdx := (leaderIdx + 1) % 3

	nodes[slowIdx].Stop()
	nodes[slowIdx] = nil

	var lastIdx uint64
	for i := 0; i < 10; i++ {
		idx, _, err := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
		lastIdx = idx
	}
	time.Sleep(300 * time.Millisecond)

	rejoined, err := NewRaftNode(configs[slowIdx])
	require.NoError(t, err)
	require.NoError(t, rejoined.StartGRPCServer(addresses[slowIdx]))
	rejoined.Start()
	nodes[slowIdx] = rejoined

	waitForCommit(t, nodes[slowIdx], lastIdx, 3*time.Second)
	assert.Equal(t, getLog(nodes[leaderIdx]), getLog(nodes[slowIdx]))
}

func TestCommitRequiresMajority(t *testing.T) {
	configs, addresses := clusterConfig(t, 3)
	nodes := startFromConfigs(t, configs, addresses)
	defer stopCluster(nodes)

	leaderIdx := waitForLeader(t, nodes, 3*time.Second)

	for i := range nodes {
		if i != leaderIdx {
			nodes[i].Stop()
			nodes[i] = nil
		}
	}

	idx, _, err := nodes[leaderIdx].Propose([]byte("no-quorum"))
	require.NoError(t, err)

	nodes[leaderIdx].mu.RLock()
	assert.Equal(t, idx, uint64(len(nodes[leaderIdx].log)-1), "entry should be in leader log")
	nodes[leaderIdx].mu.RUnlock()

	consistently(t, 500*time.Millisecond, func() bool {
		n := nodes[leaderIdx]
		n.mu.RLock()
		defer n.mu.RUnlock()
		return n.commitIndex < idx
	})

}
