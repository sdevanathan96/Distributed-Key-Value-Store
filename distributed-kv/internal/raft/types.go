package raft

import (
	"encoding/json"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// String returns the human readable name of the node state.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type RaftConfig struct {
	NodeID             string
	Peers              map[string]string
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	DataDir            string
}

// DefaultConfig returns a RaftConfig for nodeID with the given peer address
// map and the standard election and heartbeat timings.
func DefaultConfig(nodeID string, peers map[string]string) RaftConfig {
	return RaftConfig{
		NodeID:             nodeID,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            "/data",
	}
}

type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

type ApplyMsg struct {
	CommandValid  bool
	Command       []byte
	CommandIndex  uint64
	CommandTerm   uint64
	SnapshotTerm  uint64
	SnapshotIndex uint64
	SnapshotValid bool
	Snapshot      []byte
}

type RaftCommand struct {
	Op    string `json:"op"`
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// EncodeCommand serializes a RaftCommand to JSON for storage in a log entry.
func EncodeCommand(cmd RaftCommand) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes a RaftCommand from the JSON form produced by
// EncodeCommand.
func DecodeCommand(data []byte) (RaftCommand, error) {
	var cmd RaftCommand
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
