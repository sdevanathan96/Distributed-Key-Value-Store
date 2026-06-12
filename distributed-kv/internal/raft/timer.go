package raft

import (
	"math/rand"
	"time"
)

// randomElectionTimeout returns a duration drawn uniformly from the configured
// election timeout range.
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	min_ := rn.config.ElectionTimeoutMin
	max_ := rn.config.ElectionTimeoutMax
	spread := max_ - min_
	return min_ + time.Duration(rand.Int63n(int64(spread)))
}

// resetElectionTimer stops and drains the election timer, then restarts it with
// a fresh random timeout. The caller must hold rn.mu.
func (rn *RaftNode) resetElectionTimer() {
	if !rn.electionTimer.Stop() {
		select {
		case <-rn.electionTimer.C:
		default:
		}
	}
	rn.electionTimer.Reset(rn.randomElectionTimeout())
}

// startHeartbeatTimer launches a goroutine that sends heartbeats at the
// configured interval while the node remains leader. The goroutine exits when
// the node steps down or stops. The caller must hold rn.mu.
func (rn *RaftNode) startHeartbeatTimer() {
	rn.heartbeatStop = make(chan struct{})
	stop := rn.heartbeatStop
	go func() {
		ticker := time.NewTicker(rn.config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rn.mu.Lock()
				if rn.state == Leader {
					rn.sendHeartbeats()
				}
				rn.mu.Unlock()
			case <-stop:
				return
			case <-rn.stopCh:
				return
			}
		}
	}()
}

// stopHeartbeatTimer signals the heartbeat goroutine to exit, if one is
// running. The caller must hold rn.mu.
func (rn *RaftNode) stopHeartbeatTimer() {
	if rn.heartbeatStop != nil {
		close(rn.heartbeatStop)
		rn.heartbeatStop = nil
	}
}
