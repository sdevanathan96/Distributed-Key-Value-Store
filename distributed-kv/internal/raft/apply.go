package raft

import "time"

// applyLoop delivers committed but not yet applied log entries to applyCh in
// index order, polling until the node is stopped. It runs as its own goroutine
// started by Start.
func (rn *RaftNode) applyLoop() {
	defer close(rn.applyDone)
	for {
		select {
		case <-rn.stopCh:
			return
		default:
		}

		rn.mu.Lock()
		var entriesToApply []LogEntry
		for rn.lastApplied < rn.commitIndex && rn.lastApplied+1 < uint64(len(rn.log)) {
			rn.lastApplied++
			entriesToApply = append(entriesToApply, rn.log[rn.lastApplied])
		}

		rn.mu.Unlock()

		for _, entry := range entriesToApply {
			select {
			case rn.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}:
			case <-rn.stopCh:
				return
			}
		}
		if len(entriesToApply) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
