package raft

import (
	"context"
	"distributed-kv/proto/raftpb"
	"log"
	"sync/atomic"
	"time"
)

// startElection advances to a new term, votes for itself, and requests votes
// from all peers in parallel, becoming leader if it collects a majority. The
// caller must hold rn.mu.
func (rn *RaftNode) startElection() {
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.leaderId = ""
	rn.resetElectionTimer()
	lastIndex, lastTerm := rn.getLastLogInfo()
	err := rn.persist()
	if err != nil {
		log.Printf("%s: failed to persist during election: %v", rn.id, err)
		return
	}
	req := &raftpb.VoteRequest{
		Term:         rn.currentTerm,
		CandidateId:  rn.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	votesNeeded := (len(rn.peers)+1)/2 + 1
	var votesReceived atomic.Int32
	votesReceived.Add(1)
	currentTerm := rn.currentTerm

	for peerID, client := range rn.peers {
		go func(peerID string, client raftpb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				log.Printf("%s: RequestVote to %s failed: %v", rn.id, peerID, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if rn.currentTerm != currentTerm || rn.state != Candidate {
				return
			}

			if resp.Term > rn.currentTerm {
				rn.becomeFollower(resp.Term)
				return
			}

			if resp.VoteGranted {
				total := votesReceived.Add(1)
				if int(total) >= votesNeeded {
					rn.becomeLeader()
				}
			}
		}(peerID, client)
	}
	if int(votesReceived.Load()) >= votesNeeded {
		rn.becomeLeader()
	}
}
