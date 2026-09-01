package raftnode

import (
	"encoding/binary"
)

// votedForFromStore reads this node's persistent vote for the current term
// from the Raft stable store: LastVoteTerm (big-endian uint64) + LastVoteCand
// (the candidate's raft address). A vote from an older term is never reported.
func (n *Node) votedForFromStore() string {
	termBytes, err := n.boltStore.Get([]byte("LastVoteTerm"))
	if err != nil || len(termBytes) != 8 {
		return ""
	}
	if binary.BigEndian.Uint64(termBytes) != n.raft.CurrentTerm() {
		return "" // vote belongs to an older term
	}
	candBytes, err := n.boltStore.Get([]byte("LastVoteCand"))
	if err != nil || len(candBytes) == 0 {
		return ""
	}
	// Stored as a raft address; resolve to a node ID via this node's own view
	return n.nodeIDForRaftAddr(string(candBytes))
}
