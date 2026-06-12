package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type PersistedState struct {
	CurrentTerm uint64     `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

// doPersist writes the node's term, vote, and log to disk atomically by writing
// a temp file, syncing it, and renaming it into place. The caller must hold
// rn.mu.
func (rn *RaftNode) doPersist() error {
	state := PersistedState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
		Log:         rn.log,
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	statePath := filepath.Join(rn.config.DataDir, "raft-state.json")
	tmpPath := statePath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("persist open: %w", err)
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("persist write: %w", err)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("persist sync: %w", err)
	}
	f.Close()
	if err := os.Rename(tmpPath, statePath); err != nil {
		return fmt.Errorf("persist rename: %w", err)
	}
	dir, err := os.Open(rn.config.DataDir)
	if err != nil {
		return fmt.Errorf("persist open dir: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("persist sync dir: %w", err)
	}
	return nil

}

// loadPersisted restores term, vote, and log from the state file on disk. A
// missing file is not an error and leaves the node at its initial state.
func (rn *RaftNode) loadPersisted() error {
	statePath := filepath.Join(rn.config.DataDir, "raft-state.json")
	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var state PersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor
	rn.log = state.Log
	return nil
}

// persist wraps doPersist and sends any error to the fatalCh to trigger a shutdown.
// The caller must hold rn.mu.
func (rn *RaftNode) persist() error {
	err := rn.doPersist()
	if err != nil {
		rn.fatal(err)
	}
	return err
}
