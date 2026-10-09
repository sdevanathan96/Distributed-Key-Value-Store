package raft

import (
	"distributed-kv/proto/raftpb"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

const applyChBuffer = 256

var ErrNotLeader = errors.New("not leader")

type RaftNode struct {
	currentTerm   uint64
	votedFor      string
	log           []LogEntry
	commitIndex   uint64
	lastApplied   uint64
	nextIndex     map[string]uint64
	matchIndex    map[string]uint64
	mu            sync.RWMutex
	id            string
	state         NodeState
	config        RaftConfig
	leaderId      string
	applyCh       chan ApplyMsg
	peers         map[string]raftpb.RaftServiceClient
	electionTimer *time.Timer
	stopCh        chan struct{}
	applyDone     chan struct{}
	stopped       bool
	heartbeatStop chan struct{}
	fatalCh       chan error
	grpcServer    *grpc.Server
	peerConns     []*grpc.ClientConn
	// lastLeaderContact is when a follower last heard from its current leader.
	// While it is recent, higher term vote requests are ignored.
	lastLeaderContact time.Time
	// peerContact is when a leader last got a response from each peer, for
	// CheckQuorum. Reset in becomeLeader.
	peerContact map[string]time.Time
	// preVoteRound invalidates responses that arrive after a newer pre-vote
	// round has started.
	preVoteRound uint64
}

type Status struct {
	NodeID   string
	LeaderID string
	Term     uint64
	Nodes    []string
}

func (rn *RaftNode) Status() Status {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	nodes := make([]string, 0, len(rn.config.Peers)+1)
	nodes = append(nodes, rn.id)
	for peerID := range rn.config.Peers {
		nodes = append(nodes, peerID)
	}
	return Status{
		NodeID:   rn.id,
		LeaderID: rn.leaderId,
		Term:     rn.currentTerm,
		Nodes:    nodes,
	}
}

// NewRaftNode constructs a follower RaftNode from config, restoring any
// persisted term, vote, and log from DataDir. Committed commands are delivered
// in order on applyCh. It starts no goroutines; call Start for that.
func NewRaftNode(config RaftConfig) (*RaftNode, error) {
	var node = new(RaftNode)
	node.currentTerm = 0
	node.votedFor = ""
	node.log = []LogEntry{{Term: 0, Index: 0}}
	node.commitIndex = 0
	node.lastApplied = 0
	node.state = Follower
	node.peers = make(map[string]raftpb.RaftServiceClient)
	node.nextIndex = make(map[string]uint64)
	node.matchIndex = make(map[string]uint64)
	node.id = config.NodeID
	node.config = config
	node.leaderId = ""
	node.applyCh = make(chan ApplyMsg, applyChBuffer) // raft owns it now
	node.stopCh = make(chan struct{})
	node.applyDone = make(chan struct{})
	node.fatalCh = make(chan error, 1)
	node.stopped = false
	node.peerConns = []*grpc.ClientConn{}
	timeout := config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(config.ElectionTimeoutMax-config.ElectionTimeoutMin)))
	node.electionTimer = time.NewTimer(timeout)
	node.grpcServer = nil
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		node.electionTimer.Stop()
		return nil, fmt.Errorf("create raft data dir %q: %w", config.DataDir, err)
	}
	// Starting blank on a bad state file would forget this node's vote and log,
	// letting it vote twice in one term. Refuse to start instead.
	if err := node.loadPersisted(); err != nil {
		node.electionTimer.Stop()
		return nil, fmt.Errorf("load raft state: %w", err)
	}
	// The state machine can only have applied entries this node already held
	// in its log, so an applied index past the end means the raft and storage
	// directories do not belong together.
	lastIndex, _ := node.getLastLogInfo()
	if config.AppliedIndex > lastIndex {
		node.electionTimer.Stop()
		return nil, fmt.Errorf("state machine applied index %d is past the last raft log index %d", config.AppliedIndex, lastIndex)
	}
	node.lastApplied = config.AppliedIndex
	node.commitIndex = config.AppliedIndex
	return node, nil
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state == Leader
}

func (rn *RaftNode) LeaderHint() (addr string, known bool) {
	rn.mu.RLock()
	leaderID := rn.leaderId
	rn.mu.RUnlock()
	if leaderID == "" {
		return "", false
	}
	addr, ok := rn.config.Peers[leaderID]
	if !ok {
		return "", false
	}
	return addr, true
}

// Start dials every peer, arms the election timer, and launches the main run
// loop and the apply loop. It must be called once after NewRaftNode.
func (rn *RaftNode) Start() {
	for peerId, addr := range rn.config.Peers {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// gRPC's default reconnect backoff grows to 120s. A peer that was
			// down for a while would then stay unreachable for that long after
			// it restarts, and miss every heartbeat in between. Cap it at 1s.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   time.Second,
				},
				MinConnectTimeout: time.Second,
			}),
		)
		if err != nil {
			log.Printf("%s: failed to connect to peer %s at %s", rn.id, peerId, addr)
			continue
		}
		client := raftpb.NewRaftServiceClient(conn)
		rn.peers[peerId] = client
		rn.peerConns = append(rn.peerConns, conn)
	}
	timeout := rn.config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(rn.config.ElectionTimeoutMax-rn.config.ElectionTimeoutMin)))
	rn.electionTimer.Reset(timeout)
	go rn.run()
	go rn.applyLoop()
}

// Stop halts the node: it signals the run and apply loops to exit, stops the
// timers, and gracefully shuts down the gRPC server.
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	rn.stopped = true
	rn.mu.Unlock()
	close(rn.stopCh)
	rn.electionTimer.Stop()
	rn.stopHeartbeatTimer()
	if rn.grpcServer != nil {
		rn.grpcServer.GracefulStop()
	}
	for _, conn := range rn.peerConns {
		conn.Close()
	}
	<-rn.applyDone
	close(rn.applyCh)
}

// run is the main event loop. It starts an election whenever the election
// timer fires and the node is not already leader, and returns when the node is
// stopped.
func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.electionTimer.C:
			rn.mu.Lock()
			if rn.state != Leader {
				rn.startPreVote()
			}
			rn.mu.Unlock()

		case <-rn.stopCh:
			return

		case err := <-rn.fatalCh:
			log.Printf("%s: fatal persist failure, stopping: %v", rn.id, err)
			go rn.Stop()
			return
		}
	}
}

// getLastLogInfo returns the index and term of the last entry in the log. The
// caller must hold rn.mu.
func (rn *RaftNode) getLastLogInfo() (lastIndex uint64, lastTerm uint64) {
	lastEntry := rn.log[len(rn.log)-1]
	return lastEntry.Index, lastEntry.Term
}

// becomeFollower steps the node down to Follower at term, clearing its vote
// and known leader, stopping heartbeats, and rearming the election timer. The
// caller must hold rn.mu.
func (rn *RaftNode) becomeFollower(term uint64) error {
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.leaderId = ""
	rn.stopHeartbeatTimer()
	rn.resetElectionTimer()
	err := rn.persist()
	if err != nil {
		return err
	}
	return nil
}

// becomeLeader transitions the node to Leader for the current term. It
// reinitializes nextIndex and matchIndex for every peer, sends an immediate
// round of heartbeats to assert leadership, and starts the heartbeat timer.
// The caller must hold rn.mu.
func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	rn.leaderId = rn.id
	lastIndex, _ := rn.getLastLogInfo()
	// Every peer starts with fresh contact: a new leader gets one CheckQuorum
	// window to hear from a majority before it steps down.
	now := time.Now()
	rn.peerContact = make(map[string]time.Time, len(rn.peers))
	for peerId := range rn.peers {
		rn.nextIndex[peerId] = lastIndex + 1
		rn.matchIndex[peerId] = 0
		rn.peerContact[peerId] = now
	}
	rn.electionTimer.Stop()
	rn.sendHeartbeats()
	rn.startHeartbeatTimer()
}

// stepDown turns a leader that lost contact with a majority into a follower in
// the same term (CheckQuorum). Unlike becomeFollower it keeps votedFor: this
// node voted for itself in this term, and forgetting that would let it vote a
// second time in the same term. Term and vote are unchanged, so nothing is
// persisted. The caller must hold rn.mu.
func (rn *RaftNode) stepDown() {
	rn.state = Follower
	rn.leaderId = ""
	rn.stopHeartbeatTimer()
	rn.resetElectionTimer()
}

// hasLiveLeader reports whether this node believes a leader is currently in
// charge: it is the leader (CheckQuorum steps it down once it loses a
// majority), or it is a follower that heard from its leader within the
// minimum election timeout. While true, the node refuses pre-votes and
// ignores higher term vote requests (Raft thesis 4.2.3). The caller must hold
// rn.mu.
func (rn *RaftNode) hasLiveLeader() bool {
	switch rn.state {
	case Leader:
		return true
	case Follower:
		return rn.leaderId != "" && time.Since(rn.lastLeaderContact) < rn.config.ElectionTimeoutMin
	default:
		return false
	}
}

// hasQuorumContact reports whether a majority (counting this node) responded
// within the maximum election timeout. A leader that fails this has most
// likely been partitioned away, and followers on the other side may already
// have elected someone else. The caller must hold rn.mu.
func (rn *RaftNode) hasQuorumContact() bool {
	count := 1
	for peerID := range rn.peers {
		if time.Since(rn.peerContact[peerID]) < rn.config.ElectionTimeoutMax {
			count++
		}
	}
	return count >= (len(rn.peers)+1)/2+1
}

// Propose appends command to the leader's log and triggers replication,
// returning the new entry's index and term. It returns an error if this node
// is not the leader.
func (rn *RaftNode) Propose(command []byte) (uint64, uint64, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state != Leader {
		return 0, 0, ErrNotLeader
	}
	lastIndex, _ := rn.getLastLogInfo()
	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   lastIndex + 1,
		Command: command,
	}
	rn.log = append(rn.log, entry)
	err := rn.persist()
	if err != nil {
		return 0, 0, err
	}
	rn.advanceCommitIndex()
	go rn.triggerReplication()
	return entry.Index, entry.Term, nil

}

// triggerReplication fans out an AppendEntries round to every peer in
// parallel. It performs network I/O and must be called without holding rn.mu.
func (rn *RaftNode) triggerReplication() {
	for peerID, client := range rn.peers {
		go rn.replicateToFollower(peerID, client)
	}
}

func (rn *RaftNode) fatal(err error) {
	select {
	case rn.fatalCh <- err:
	default:
	}
}
