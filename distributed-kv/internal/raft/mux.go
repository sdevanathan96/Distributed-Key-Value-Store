package raft

import (
	"context"
	"distributed-kv/proto/raftpb"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mux serves the Raft RPCs of every shard a process hosts on one port. Each
// request carries its shard id and the mux hands it to that shard's node. One
// port per process (rather than one per shard) keeps the client port at raft
// port + 1 and lets every shard reuse the same node level Peers map.
//
// The shard set is fixed at construction, so dispatch reads the map without a
// lock.
type Mux struct {
	raftpb.UnimplementedRaftServiceServer
	servers    map[uint32]*raftServer
	grpcServer *grpc.Server
}

// NewMux registers nodes by their configured ShardID. Two nodes claiming the
// same shard is a wiring bug and is rejected.
func NewMux(nodes []*RaftNode) (*Mux, error) {
	servers := make(map[uint32]*raftServer, len(nodes))
	for _, rn := range nodes {
		id := rn.config.ShardID
		if _, dup := servers[id]; dup {
			return nil, fmt.Errorf("raft mux: shard %d registered twice", id)
		}
		servers[id] = &raftServer{node: rn}
	}
	return &Mux{servers: servers}, nil
}

// Start binds address and serves in the background.
func (m *Mux) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("raft mux listen %s: %w", address, err)
	}
	m.grpcServer = grpc.NewServer()
	raftpb.RegisterRaftServiceServer(m.grpcServer, m)
	go m.grpcServer.Serve(lis)
	return nil
}

// Stop drains in flight RPCs and closes the listener. Call it before stopping
// the nodes: handlers take the node lock, and a running node always releases
// it, so the drain cannot hang.
func (m *Mux) Stop() {
	if m.grpcServer != nil {
		m.grpcServer.GracefulStop()
	}
}

func (m *Mux) route(shardID uint32) (*raftServer, error) {
	s, ok := m.servers[shardID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "shard %d is not hosted on this node", shardID)
	}
	return s, nil
}

func (m *Mux) RequestVote(ctx context.Context, req *raftpb.VoteRequest) (*raftpb.VoteResponse, error) {
	s, err := m.route(req.ShardId)
	if err != nil {
		return nil, err
	}
	return s.RequestVote(ctx, req)
}

func (m *Mux) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s, err := m.route(req.ShardId)
	if err != nil {
		return nil, err
	}
	return s.AppendEntries(ctx, req)
}

func (m *Mux) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	s, err := m.route(req.ShardId)
	if err != nil {
		return nil, err
	}
	return s.InstallSnapshot(ctx, req)
}
