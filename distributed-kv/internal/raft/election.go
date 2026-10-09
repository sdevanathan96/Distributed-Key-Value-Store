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
		ShardId:      rn.config.ShardID,
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

// startPreVote asks every peer whether it would vote for this node at the next
// term, without changing this node's term or recording a vote (Raft thesis
// 9.6). Only a majority of yes answers starts a real election. A node that
// cannot win (its log is behind) or whose peers still hear a leader never
// bumps its term, so a node rejoining after downtime cannot depose a healthy
// leader. The caller must hold rn.mu.
func (rn *RaftNode) startPreVote() {
	rn.resetElectionTimer()
	needed := (len(rn.peers)+1)/2 + 1
	if needed <= 1 {
		rn.startElection()
		return
	}
	lastIndex, lastTerm := rn.getLastLogInfo()
	term := rn.currentTerm
	rn.preVoteRound++
	round := rn.preVoteRound
	req := &raftpb.VoteRequest{
		Term:         term + 1,
		CandidateId:  rn.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		ShardId:      rn.config.ShardID,
		PreVote:      true,
	}
	var granted atomic.Int32
	granted.Add(1)

	for peerID, client := range rn.peers {
		go func(peerID string, client raftpb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				log.Printf("%s: PreVote to %s failed: %v", rn.id, peerID, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Stale if a newer round started, the term moved, or a leader
			// appeared while the request was in flight.
			if rn.preVoteRound != round || rn.currentTerm != term || rn.hasLiveLeader() {
				return
			}
			if resp.Term > rn.currentTerm {
				rn.becomeFollower(resp.Term)
				return
			}
			if resp.VoteGranted && int(granted.Add(1)) == needed {
				rn.startElection()
			}
		}(peerID, client)
	}
}

// preVoteResponse answers a pre-vote without changing any state. It grants
// only what a real vote at req.Term would grant, and refuses outright while
// this node believes a leader is live. The caller must hold rn.mu.
func (rn *RaftNode) preVoteResponse(req *raftpb.VoteRequest) *raftpb.VoteResponse {
	resp := &raftpb.VoteResponse{Term: rn.currentTerm}
	if req.Term <= rn.currentTerm || rn.hasLiveLeader() {
		return resp
	}
	lastIndex, lastTerm := rn.getLastLogInfo()
	resp.VoteGranted = req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	return resp
}
