package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv/internal/raft"
	"distributed-kv/internal/shard"
	"distributed-kv/internal/storage"
	"distributed-kv/proto/kvpb"
	"distributed-kv/proto/raftpb"
)

const proposeTimeout = 5 * time.Second

// KVAPI implements kvpb.KVServiceServer. Thin translation layer: route by key
// to a shard, enforce leader only writes and reads, map storage and raft
// errors to gRPC status codes.
type KVAPI struct {
	kvpb.UnimplementedKVServiceServer
	server *Server
}

func NewKVAPI(s *Server) *KVAPI {
	return &KVAPI{server: s}
}

func (a *KVAPI) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	sh := a.server.ShardFor(req.Key)
	if err := a.requireLeader(sh); err != nil {
		return nil, err
	}

	cmd := &raftpb.Command{
		RequestId: ulid.Make().String(),
		Op:        raftpb.Op_OP_PUT,
		Key:       req.Key,
		Value:     req.Value,
	}

	cctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()

	if err := sh.Propose(cctx, cmd); err != nil {
		return nil, a.mapProposeErr(sh, err)
	}
	return &kvpb.PutResponse{}, nil
}

func (a *KVAPI) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	sh := a.server.ShardFor(req.Key)
	if err := a.requireLeader(sh); err != nil {
		return nil, err
	}

	cmd := &raftpb.Command{
		RequestId: ulid.Make().String(),
		Op:        raftpb.Op_OP_DELETE,
		Key:       req.Key,
	}

	cctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()

	if err := sh.Propose(cctx, cmd); err != nil {
		return nil, a.mapProposeErr(sh, err)
	}
	return &kvpb.DeleteResponse{}, nil
}

func (a *KVAPI) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	sh := a.server.ShardFor(req.Key)
	if err := a.requireLeader(sh); err != nil {
		return nil, err
	}

	val, err := sh.Get(req.Key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return &kvpb.GetResponse{Found: false}, nil
	}
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("get: %v", err))
	}
	return &kvpb.GetResponse{Value: val, Found: true}, nil
}

func (a *KVAPI) ClusterStatus(ctx context.Context, req *kvpb.ClusterStatusRequest) (*kvpb.ClusterStatusResponse, error) {
	st := a.server.shards[0].Status()
	return &kvpb.ClusterStatusResponse{
		LeaderId: st.LeaderID,
		Nodes:    st.Nodes,
		Term:     st.Term,
	}, nil
}

// requireLeader returns nil if the shard is the leader, otherwise a redirect
// or unavailable status. Shared by Put, Get, Delete so the leader policy lives
// in one place.
func (a *KVAPI) requireLeader(sh *shard.Shard) error {
	if sh.IsLeader() {
		return nil
	}
	return a.notLeader(sh)
}

// mapProposeErr translates a Shard.Propose error into a gRPC status. Called
// only after requireLeader passed, so a not leader error here means leadership
// was lost between the check and the propose (a real race): re-redirect. A
// deadline means the entry did not commit and apply in time; the client
// retries (writes are idempotent). Anything else is Internal.
func (a *KVAPI) mapProposeErr(sh *shard.Shard, err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return a.notLeader(sh)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "write not committed within deadline")
	default:
		return status.Error(codes.Internal, fmt.Sprintf("propose: %v", err))
	}
}

// notLeader builds FAILED_PRECONDITION with a LeaderHint detail carrying the
// leader's client address (raft port + 1). UNAVAILABLE if no leader is known.
func (a *KVAPI) notLeader(sh *shard.Shard) error {
	raftAddr, known := sh.LeaderHint()
	if !known {
		return status.Error(codes.Unavailable, "no leader elected")
	}
	clientAddr, err := RaftAddrToClientAddr(raftAddr)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("leader addr: %v", err))
	}
	st := status.New(codes.FailedPrecondition, "not leader")
	st, werr := st.WithDetails(&kvpb.LeaderHint{ClientAddr: clientAddr})
	if werr != nil {
		return status.Error(codes.FailedPrecondition, "not leader")
	}
	return st.Err()
}

// RaftAddrToClientAddr converts "host:raftPort" to "host:raftPort+1". Uses
// net.SplitHostPort / JoinHostPort so IPv6 bracketing is handled correctly.
func RaftAddrToClientAddr(raftAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("split leader addr %q: %w", raftAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("parse leader port %q: %w", portStr, err)
	}
	return net.JoinHostPort(host, strconv.Itoa(port+1)), nil
}
