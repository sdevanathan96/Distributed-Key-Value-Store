package raft

import (
	"context"
	"distributed-kv/proto/raftpb"
	"net"

	"google.golang.org/grpc"
)

type raftServer struct {
	raftpb.UnimplementedRaftServiceServer
	node *RaftNode
}

// RequestVote handles an incoming RequestVote RPC, granting the vote only if
// the candidate's term is current and its log is at least as up to date as this
// node's.
func (s *raftServer) RequestVote(ctx context.Context, req *raftpb.VoteRequest) (*raftpb.VoteResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	resp := &raftpb.VoteResponse{
		Term:        s.node.currentTerm,
		VoteGranted: false,
	}
	if req.Term < s.node.currentTerm {
		return resp, nil
	}

	if req.Term > s.node.currentTerm {
		err := s.node.becomeFollower(req.Term)
		if err != nil {
			return resp, err
		}
		resp.Term = s.node.currentTerm
	}
	if s.node.votedFor == "" || s.node.votedFor == req.CandidateId {
		lastIndex, lastTerm := s.node.getLastLogInfo()
		upToDate := req.LastLogTerm > lastTerm ||
			(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
		if upToDate {
			s.node.votedFor = req.CandidateId
			err := s.node.persist()
			if err != nil {
				return resp, err
			}
			resp.VoteGranted = true
			s.node.resetElectionTimer()
		}
	}
	return resp, nil
}

// InstallSnapshot handles an incoming InstallSnapshot RPC. It is currently a
// stub that only reports the node's term.
func (s *raftServer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	resp := &raftpb.InstallSnapshotResponse{
		Term: s.node.currentTerm,
	}
	return resp, nil
}

// StartGRPCServer binds the Raft service to address and serves it in the
// background.
func (rn *RaftNode) StartGRPCServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	rn.grpcServer = grpc.NewServer()
	raftpb.RegisterRaftServiceServer(rn.grpcServer, &raftServer{node: rn})
	go rn.grpcServer.Serve(lis)
	return nil
}

// AppendEntries handles an incoming AppendEntries RPC. It rejects stale terms,
// steps down on newer ones, enforces the log matching property at
// PrevLogIndex, appends or overwrites the carried entries, and advances the
// commit index from LeaderCommit.
func (s *raftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {

	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	logChanged := false

	resp := &raftpb.AppendEntriesResponse{
		Term:    s.node.currentTerm,
		Success: false,
	}
	if req.Term < s.node.currentTerm {
		return resp, nil
	}
	if req.Term > s.node.currentTerm {
		err := s.node.becomeFollower(req.Term)
		if err != nil {
			return resp, err
		}
	} else {
		s.node.state = Follower
	}
	s.node.leaderId = req.LeaderId
	s.node.resetElectionTimer()
	resp.Term = s.node.currentTerm
	if req.PrevLogIndex >= uint64(len(s.node.log)) {
		return resp, nil
	}

	if s.node.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		s.node.log = s.node.log[:req.PrevLogIndex]
		return resp, s.node.persist()
	}
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)

		if idx < uint64(len(s.node.log)) {
			if s.node.log[idx].Term != entry.Term {
				s.node.log = s.node.log[:idx]
				s.node.log = append(s.node.log, LogEntry{
					Term:    entry.Term,
					Index:   entry.Index,
					Command: entry.Command,
				})
				logChanged = true
			}
		} else {
			s.node.log = append(s.node.log, LogEntry{
				Term:    entry.Term,
				Index:   entry.Index,
				Command: entry.Command,
			})
			logChanged = true
		}
	}
	if logChanged {
		err := s.node.persist()
		if err != nil {
			return resp, err
		}
	}
	if req.LeaderCommit > s.node.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			s.node.commitIndex = req.LeaderCommit
		} else {
			s.node.commitIndex = lastNewIndex
		}
	}
	resp.Success = true
	resp.MatchIndex = uint64(len(s.node.log)) - 1
	return resp, nil
}
