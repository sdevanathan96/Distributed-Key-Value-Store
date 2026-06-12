package raft

// advanceCommitIndex raises commitIndex to the highest log index replicated on
// a majority of nodes that also belongs to the current term, per the Raft
// commit rule. The caller must hold rn.mu.
func (rn *RaftNode) advanceCommitIndex() {
	for n := rn.commitIndex + 1; n < uint64(len(rn.log)); n++ {
		if rn.log[n].Term != rn.currentTerm {
			continue
		}

		count := 1
		for _, matchIdx := range rn.matchIndex {
			if matchIdx >= n {
				count++
			}
		}

		if count > (len(rn.peers)+1)/2 {
			rn.commitIndex = n
		}
	}
}
