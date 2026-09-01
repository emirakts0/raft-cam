package events

import "time"

// EventType represents the category of the Raft cluster event
type EventType string

const (
	EventStateSnapshot       EventType = "STATE_SNAPSHOT"
	EventLeaderChanged       EventType = "LEADER_CHANGED"
	EventTermChanged         EventType = "TERM_CHANGED"
	EventPeerJoined          EventType = "PEER_JOINED"
	EventPeerRemoved         EventType = "PEER_REMOVED"
	EventLogReplicated       EventType = "LOG_REPLICATED"
	EventLogApplied          EventType = "LOG_APPLIED"
	EventHeartbeatSent       EventType = "HEARTBEAT_SENT"     // real outgoing AppendEntries (empty), coalesced by Hub
	EventHeartbeatReceived   EventType = "HEARTBEAT_RECEIVED" // real incoming AppendEntries (empty), coalesced by Hub
	EventAppendEntriesSent   EventType = "APPEND_ENTRIES_SENT"
	EventAppendEntriesResult EventType = "APPEND_ENTRIES_RESULT"
	EventAppendEntriesRecv   EventType = "APPEND_ENTRIES_RECEIVED" // real incoming AppendEntries with entries (follower side)
	EventVoteRequested       EventType = "VOTE_REQUESTED"
	EventVoteGranted         EventType = "VOTE_GRANTED"
	EventVoteRejected        EventType = "VOTE_REJECTED"
	EventLeadershipTransfer  EventType = "LEADERSHIP_TRANSFER" // real TimeoutNow RPC
	EventSnapshotInstall     EventType = "SNAPSHOT_INSTALL"
	EventSnapshotCreated     EventType = "SNAPSHOT_CREATED" // real log compaction (SnapshotStore.Create)
	EventFSMRestored         EventType = "FSM_RESTORED"     // real FSM Restore from a snapshot
	EventConfigChanged       EventType = "CONFIG_CHANGED"
	EventProposalForwarded   EventType = "PROPOSAL_FORWARDED"
	EventProposalReceived    EventType = "PROPOSAL_RECEIVED"
	EventElectionStarted     EventType = "ELECTION_STARTED"
	EventLeadershipLost      EventType = "LEADERSHIP_LOST"
	EventNodeStatusChanged   EventType = "NODE_STATUS_CHANGED"
	EventSyncComplete        EventType = "SYNC_COMPLETE"
)

// Event is the payload sent over SSE
type Event struct {
	ID        string    `json:"id"`
	Type      EventType `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	NodeID    string    `json:"node_id"`
	Term      uint64    `json:"term"`
	Message   string    `json:"message"`
	Data      any       `json:"data,omitempty"`
}

// PeerInfo represents a node in the cluster
type PeerInfo struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
	Role     string `json:"role"` // "Leader", "Follower", "Candidate", "Shutdown"
	Voter    bool   `json:"voter"`
	IsLeader bool   `json:"is_leader"`
	IsSelf   bool   `json:"is_self"`
	Healthy  bool   `json:"healthy"`
}

// ClusterState represents the complete snapshot of the cluster known to a node
type ClusterState struct {
	SelfID         string            `json:"self_id"`
	SelfRole       string            `json:"self_role"` // "Leader", "Follower", "Candidate", "Shutdown"
	RaftAddr       string            `json:"raft_addr"`
	HTTPAddr       string            `json:"http_addr"`
	LeaderID       string            `json:"leader_id"`
	LeaderRaftAddr string            `json:"leader_raft_addr"`
	LeaderHTTPAddr string            `json:"leader_http_addr"`
	CurrentTerm    uint64            `json:"current_term"`
	VotedFor       string            `json:"voted_for,omitempty"` // persistent vote cast in the current term ("" = none)
	CommitIndex    uint64            `json:"commit_index"`
	LastLogIndex   uint64            `json:"last_log_index"`
	LastLogTerm    uint64            `json:"last_log_term"`
	AppliedIndex   uint64            `json:"applied_index"`
	LastContactMS  int64             `json:"last_contact_ms"` // ms since last leader contact (-1 = never/leader)
	Peers          []PeerInfo        `json:"peers"`
	ActiveClients  int               `json:"active_clients"`
	KVData         map[string]string `json:"kv_data,omitempty"`
	RaftStats      map[string]string `json:"raft_stats,omitempty"` // raw hashicorp/raft Stats() dump
	Timestamp      time.Time         `json:"timestamp"`
}
