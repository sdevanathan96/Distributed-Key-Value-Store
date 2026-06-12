package raft

import (
	"distributed-kv/proto/raftpb"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
}

// NewRaftNode constructs a follower RaftNode from config, restoring any
// persisted term, vote, and log from DataDir. Committed commands are delivered
// in order on applyCh. It starts no goroutines; call Start for that.
func NewRaftNode(config RaftConfig, applyCh chan ApplyMsg) (*RaftNode, error) {
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
	node.applyCh = applyCh
	node.stopCh = make(chan struct{})
	node.applyDone = make(chan struct{})
	node.fatalCh = make(chan error, 1)
	node.stopped = false
	node.peerConns = []*grpc.ClientConn{}
	timeout := config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(config.ElectionTimeoutMax-config.ElectionTimeoutMin)))
	node.electionTimer = time.NewTimer(timeout)
	node.grpcServer = nil
	node.loadPersisted()
	return node, nil
}

// Start dials every peer, arms the election timer, and launches the main run
// loop and the apply loop. It must be called once after NewRaftNode.
func (rn *RaftNode) Start() {
	for peerId, addr := range rn.config.Peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
				rn.startElection()
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
	for peerId := range rn.peers {
		rn.nextIndex[peerId] = lastIndex + 1
		rn.matchIndex[peerId] = 0
	}
	rn.electionTimer.Stop()
	rn.sendHeartbeats()
	rn.startHeartbeatTimer()
}

// Propose appends command to the leader's log and triggers replication,
// returning the new entry's index and term. It returns an error if this node
// is not the leader.
func (rn *RaftNode) Propose(command []byte) (uint64, uint64, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state != Leader {
		return 0, 0, fmt.Errorf("not the leader, leader is %s", rn.leaderId)
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
