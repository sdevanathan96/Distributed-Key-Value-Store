package raft

import (
	"context"
	"distributed-kv/proto/raftpb"
	"time"
)

// replicateToFollower sends one AppendEntries RPC to peerID carrying every
// entry from the follower's nextIndex to the end of the leader's log, or an
// empty batch if the follower is caught up. On success it advances the peer's
// matchIndex and nextIndex and attempts to advance the commit index; on a log
// inconsistency it backs nextIndex off by one for the next attempt. A response
// carrying a higher term steps the node down.
//
// Must be called without holding rn.mu, since it performs network I/O. It
// acquires rn.mu only to read leader state before the RPC and to apply the
// result after.
func (rn *RaftNode) replicateToFollower(peerID string, client raftpb.RaftServiceClient) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}
	peerNextIndex := rn.nextIndex[peerID]
	prevLogIndex := peerNextIndex - 1
	prevLogTerm := rn.log[prevLogIndex].Term
	currentTerm := rn.currentTerm
	var entries []*raftpb.LogEntry
	for i := peerNextIndex; i < uint64(len(rn.log)); i++ {
		entries = append(entries, &raftpb.LogEntry{
			Term:    rn.log[i].Term,
			Index:   rn.log[i].Index,
			Command: rn.log[i].Command,
		})
	}
	leaderCommit := rn.commitIndex
	rn.mu.Unlock()

	req := &raftpb.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     rn.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return
	}
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.currentTerm != currentTerm || rn.state != Leader {
		return
	}

	if resp.Term > rn.currentTerm {
		rn.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		newMatchIndex := prevLogIndex + uint64(len(entries))
		if newMatchIndex > rn.matchIndex[peerID] {
			rn.matchIndex[peerID] = newMatchIndex
		}
		rn.nextIndex[peerID] = newMatchIndex + 1

		rn.advanceCommitIndex()
	} else {
		if rn.nextIndex[peerID] > 1 {
			rn.nextIndex[peerID]--
		}
	}
}

// sendHeartbeats triggers an AppendEntries round to every peer. It is called
// with rn.mu held; spawns the per peer goroutines.
func (rn *RaftNode) sendHeartbeats() {
	for peerID, client := range rn.peers {
		go rn.replicateToFollower(peerID, client)
	}
}
