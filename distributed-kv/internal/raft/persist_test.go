package raft

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// persistedConfig returns a single node config whose data dir already holds
// stateJSON as raft-state.json.
func persistedConfig(t *testing.T, stateJSON string) RaftConfig {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "raft-state.json"), []byte(stateJSON), 0o644))
	return RaftConfig{
		NodeID:             "node-0",
		Peers:              map[string]string{},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            dir,
	}
}

func TestNewRaftNodeRestoresPersistedState(t *testing.T) {
	cfg := persistedConfig(t, `{"currentTerm":4,"votedFor":"node-2","log":[`+
		`{"term":0,"index":0},{"term":2,"index":1,"command":"YQ=="},{"term":4,"index":2,"command":"Yg=="}]}`)

	rn, err := NewRaftNode(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { rn.electionTimer.Stop() })

	assert.Equal(t, uint64(4), rn.currentTerm)
	assert.Equal(t, "node-2", rn.votedFor)
	require.Len(t, rn.log, 3)
	assert.Equal(t, []byte("b"), rn.log[2].Command)
}

// An unreadable state file used to be ignored, starting the node at term 0
// with no vote and an empty log. That node could vote twice in a term it had
// already voted in, and would see every candidate's log as up to date.
func TestNewRaftNodeRejectsCorruptState(t *testing.T) {
	_, err := NewRaftNode(persistedConfig(t, `{"currentTerm":4,"votedFor":"node-2","log":[{"ter`))
	assert.Error(t, err)
}

// Parses fine but has no index 0 sentinel; getLastLogInfo would index -1.
func TestNewRaftNodeRejectsStateWithoutSentinel(t *testing.T) {
	_, err := NewRaftNode(persistedConfig(t, `{"currentTerm":4,"votedFor":"node-2","log":[]}`))
	assert.Error(t, err)
}

// Every code path indexes the log by position, so position must equal index.
func TestNewRaftNodeRejectsNonContiguousLog(t *testing.T) {
	_, err := NewRaftNode(persistedConfig(t, `{"currentTerm":4,"votedFor":"","log":[`+
		`{"term":0,"index":0},{"term":2,"index":2}]}`))
	assert.Error(t, err)
}

// A restarted node used to start applying from index 1, replaying writes the
// state machine already held over newer state. With AppliedIndex it resumes
// after what the state machine reports.
func TestRestartResumesAfterAppliedIndex(t *testing.T) {
	cfg := persistedConfig(t, `{"currentTerm":1,"votedFor":"node-0","log":[`+
		`{"term":0,"index":0},{"term":1,"index":1,"command":"YQ=="},`+
		`{"term":1,"index":2,"command":"Yg=="},{"term":1,"index":3,"command":"Yw=="}]}`)
	cfg.AppliedIndex = 2

	rn, err := NewRaftNode(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), rn.lastApplied)
	assert.Equal(t, uint64(2), rn.commitIndex, "applied implies committed")

	rn.Start()
	t.Cleanup(rn.Stop)
	eventually(t, 2*time.Second, rn.IsLeader)

	// Entry 3 is from an older term, so it commits along with the first entry
	// of the new term. Both arrive, and nothing at or below the applied index.
	idx, _, err := rn.Propose([]byte("d"))
	require.NoError(t, err)
	require.Equal(t, uint64(4), idx)
	for _, want := range []uint64{3, 4} {
		select {
		case msg := <-rn.ApplyCh():
			assert.Equal(t, want, msg.CommandIndex)
		case <-time.After(2 * time.Second):
			t.Fatalf("index %d was not applied", want)
		}
	}
}

// The state machine can only apply what this node's log held, so an applied
// index past the end means the raft and storage dirs do not belong together.
func TestNewRaftNodeRejectsAppliedIndexPastLog(t *testing.T) {
	cfg := persistedConfig(t, `{"currentTerm":1,"votedFor":"","log":[{"term":0,"index":0},{"term":1,"index":1}]}`)
	cfg.AppliedIndex = 5
	_, err := NewRaftNode(cfg)
	assert.Error(t, err)
}
