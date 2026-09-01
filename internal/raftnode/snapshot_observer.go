package raftnode

import (
	"fmt"
	"io"

	"raft-algo/internal/events"

	"github.com/hashicorp/raft"
)

// eventSnapshotStore wraps raft.SnapshotStore and emits SNAPSHOT_CREATED when
// the Raft engine persists a snapshot (log compaction point). Raft has no
// observation channel for this lifecycle step, so the store itself — a real
// Raft extension point handed to raft.NewRaft — is the observation surface.
type eventSnapshotStore struct {
	inner raft.SnapshotStore
	node  *Node
}

// newEventSnapshotStore wraps the given raw snapshot store with compaction event emission
func newEventSnapshotStore(inner raft.SnapshotStore, n *Node) *eventSnapshotStore {
	return &eventSnapshotStore{inner: inner, node: n}
}

// Create intercepts the real snapshot creation performed by the Raft engine
func (s *eventSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	sink, err := s.inner.Create(version, index, term, configuration, configurationIndex, trans)
	if err == nil {
		s.node.hub.Broadcast(events.Event{
			NodeID: s.node.ID(),
			Term:   term,
			Type:   events.EventSnapshotCreated,
			Message: fmt.Sprintf("Snapshot created at index %d (Term %d) — log compacted, %d voter(s) in configuration",
				index, term, countVoters(configuration)),
			Data: map[string]any{
				"index":            index,
				"term":             term,
				"config_index":     configurationIndex,
				"voters":           countVoters(configuration),
				"snapshot_version": version,
			},
		})
	}
	return sink, err
}

// List returns the available snapshots (passthrough)
func (s *eventSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return s.inner.List()
}

// Open opens a snapshot by ID (passthrough)
func (s *eventSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return s.inner.Open(id)
}

func countVoters(configuration raft.Configuration) int {
	count := 0
	for _, s := range configuration.Servers {
		if s.Suffrage == raft.Voter {
			count++
		}
	}
	return count
}
