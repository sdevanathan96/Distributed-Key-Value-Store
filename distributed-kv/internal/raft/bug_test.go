package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyLoopRespectsLogBounds(t *testing.T) {
	rn := &RaftNode{
		log: []LogEntry{
			{Term: 0, Index: 0},
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 1, Index: 2, Command: []byte("b")},
		},
		commitIndex: 10,
		lastApplied: 0,
		applyCh:     make(chan ApplyMsg, 100),
		stopCh:      make(chan struct{}),
		applyDone:   make(chan struct{}),
	}

	go rn.applyLoop()

	eventually(t, 1*time.Second, func() bool {
		return len(rn.applyCh) == len(rn.log)-1
	})

	rn.mu.RLock()
	lastApplied := rn.lastApplied
	rn.mu.RUnlock()

	close(rn.stopCh)

	assert.LessOrEqual(t, lastApplied, uint64(len(rn.log)-1))
	assert.Equal(t, len(rn.log)-1, len(rn.applyCh))
}

func TestStopShutdownDoesNotPanic(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	leaderIdx := waitForLeader(t, nodes, 3*time.Second)

	for i := 0; i < 100; i++ {
		nodes[leaderIdx].Propose([]byte(fmt.Sprintf("cmd-%d", i)))
	}
	time.Sleep(50 * time.Millisecond)

	for _, n := range nodes {
		n.Stop()
	}
}

func TestStopIsIdempotent(t *testing.T) {
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

	node.Stop()
	node.Stop()
}
