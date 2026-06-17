package shard

import (
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"

	"distributed-kv/internal/raft"
	"distributed-kv/proto/raftpb"
)

// consumeApply reads applied Raft commands and dispatches them to storage,
// signaling waiters on success or error. Takes the channel as a parameter so
// tests can inject one without a real RaftNode; production passes
// raft.ApplyCh(). Exits when the channel is closed (raft.Stop closes it after
// applyLoop drains), which also drains any buffered entries first.
//
// Invariants (CLAUDE.md "Pending write waiters"):
//   - NEVER delete from the waiters map here. Waiter side owns that.
//   - Signal by send on a size 1 buffered channel, never by close.
//   - waitersMu held only across the map lookup, never across storage I/O.
//   - Empty or missing RequestID -> skip signal silently.
func (sh *Shard) consumeApply(applyCh <-chan raft.ApplyMsg) {
	defer sh.wg.Done()

	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}

		var cmd raftpb.Command
		if err := proto.Unmarshal(msg.Command, &cmd); err != nil {
			log.Printf("shard %d: unmarshal: %v", sh.ID, err)
			continue
		}

		var applyErr error
		switch cmd.Op {
		case raftpb.Op_OP_PUT:
			applyErr = sh.engine.Put(cmd.Key, cmd.Value)
		case raftpb.Op_OP_DELETE:
			applyErr = sh.engine.Delete(cmd.Key)
		default:
			applyErr = fmt.Errorf("unknown op: %v", cmd.Op)
			log.Printf("shard %d: %v", sh.ID, applyErr)
		}

		if cmd.RequestId != "" {
			sh.waitersMu.Lock()
			ch, found := sh.waiters[cmd.RequestId]
			sh.waitersMu.Unlock()
			if found {
				ch <- applyErr
			}
		}
	}
}
