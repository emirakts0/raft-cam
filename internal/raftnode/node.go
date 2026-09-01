package raftnode

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"raft-algo/internal/events"
	"raft-algo/internal/fsm"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Config holds configuration parameters for a Raft node
type Config struct {
	NodeID    string
	RaftAddr  string
	HTTPAddr  string
	DataDir   string
	Bootstrap bool
	JoinAddr  string
}

// Node wraps hashicorp/raft and orchestrates cluster state & event streaming
type Node struct {
	cfg       Config
	raft      *raft.Raft
	transport *raft.NetworkTransport
	fsm       *fsm.KVStoreFSM
	hub       *events.Hub
	boltStore *raftboltdb.BoltStore
	obsCh     chan raft.Observation
	observer  *raft.Observer

	mu            sync.RWMutex
	peerHTTPAddrs map[string]string
	peerHealth    map[string]bool
	knownPeers    map[string]string
	hbFailedPeers map[string]bool // peers with a currently-failing heartbeat (transition tracking)
	lastTerm      uint64
	lastLeader    string
	lastState     raft.RaftState

	pollCancel     chan struct{}
	pollingRunning atomic.Bool
	stopCh         chan struct{}
}

// NewNode initializes and starts a Raft node
func NewNode(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node-id is required")
	}
	if cfg.RaftAddr == "" {
		return nil, fmt.Errorf("raft-addr is required")
	}
	if cfg.HTTPAddr == "" {
		return nil, fmt.Errorf("http-addr is required")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join("data", cfg.NodeID)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	node := &Node{
		cfg:           cfg,
		fsm:           fsm.NewKVStoreFSM(),
		peerHTTPAddrs: map[string]string{cfg.NodeID: cfg.HTTPAddr},
		peerHealth:    map[string]bool{cfg.NodeID: true},
		knownPeers:    map[string]string{cfg.NodeID: cfg.RaftAddr},
		hbFailedPeers: make(map[string]bool),
		stopCh:        make(chan struct{}),
	}

	node.hub = events.NewHub(node.onSSEActivated, node.onSSEDeactivated)

	node.fsm.RegisterApplyCallback(func(entry fsm.AppliedEntry) {
		node.hub.Broadcast(events.Event{
			Type:    events.EventLogApplied,
			NodeID:  cfg.NodeID,
			Term:    entry.Term,
			Message: fmt.Sprintf("Applied log %d (Term %d): %s key '%s'", entry.Index, entry.Term, entry.Command.Op, entry.Command.Key),
			Data:    entry,
		})
	})

	// Committed LogConfiguration entries arrive via raft.ConfigurationStore, never via Apply
	node.fsm.RegisterApplyConfigCallback(func(entry fsm.ConfigEntry) {
		node.hub.Broadcast(events.Event{
			Type:    events.EventConfigChanged,
			NodeID:  cfg.NodeID,
			Term:    node.raft.CurrentTerm(),
			Message: fmt.Sprintf("Cluster configuration changed (log %d): %d server(s)", entry.Index, len(entry.Servers)),
			Data:    entry,
		})
	})

	// Fires on real FSM.Restore calls; the raft engine may not exist yet during
	// startup restores, so the term is best-effort
	node.fsm.RegisterRestoreCallback(func(keys int) {
		var term uint64
		if node.raft != nil {
			term = node.raft.CurrentTerm()
		}
		node.hub.Broadcast(events.Event{
			Type:    events.EventFSMRestored,
			NodeID:  cfg.NodeID,
			Term:    term,
			Message: fmt.Sprintf("FSM restored from snapshot: %d key(s) recovered", keys),
			Data:    map[string]any{"keys": keys},
		})
	})

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 300 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 300 * time.Millisecond
	raftConfig.SnapshotInterval = 120 * time.Second
	raftConfig.SnapshotThreshold = 8192

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 5, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create tcp transport: %w", err)
	}
	node.transport = transport

	// Wrap the transport with RPC-level event emission BEFORE handing it to
	// raft.NewRaft so every outgoing/incoming RPC is observable
	obsTransport := newEventTransport(transport, node)

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	node.boltStore = boltStore

	fileSnapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}
	snapshotStore := newEventSnapshotStore(fileSnapshotStore, node)

	if cfg.Bootstrap {
		hasExistingState, err := raft.HasExistingState(boltStore, boltStore, fileSnapshotStore)
		if err != nil {
			return nil, fmt.Errorf("failed to check existing state: %w", err)
		}
		if !hasExistingState {
			log.Printf("[Raft] Bootstrapping fresh cluster on node %s (%s)...", cfg.NodeID, cfg.RaftAddr)
			configuration := raft.Configuration{
				Servers: []raft.Server{{
					ID:       raft.ServerID(cfg.NodeID),
					Address:  raft.ServerAddress(cfg.RaftAddr),
					Suffrage: raft.Voter,
				}},
			}
			if err := raft.BootstrapCluster(raftConfig, boltStore, boltStore, snapshotStore, obsTransport, configuration); err != nil {
				return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
			}
		}
	}

	r, err := raft.NewRaft(raftConfig, node.fsm, boltStore, boltStore, snapshotStore, obsTransport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	node.raft = r

	node.obsCh = make(chan raft.Observation, 100)
	node.observer = raft.NewObserver(node.obsCh, false, nil)
	node.raft.RegisterObserver(node.observer)

	go node.handleObservations()
	go node.watchLeaderCh()
	go node.continuousHealthCheck()

	return node, nil
}

// Hub returns the event hub
func (n *Node) Hub() *events.Hub {
	return n.hub
}

// FSM returns the underlying key-value FSM
func (n *Node) FSM() *fsm.KVStoreFSM {
	return n.fsm
}

// ID returns the local node ID
func (n *Node) ID() string {
	return n.cfg.NodeID
}

// RaftAddr returns the local Raft TCP address
func (n *Node) RaftAddr() string {
	return n.cfg.RaftAddr
}

// HTTPAddr returns the local HTTP address
func (n *Node) HTTPAddr() string {
	return n.cfg.HTTPAddr
}

// nodeIDForRaftAddr resolves a Raft TCP address to a node ID using this
// node's subjective knownPeers view (falls back to the raw address)
func (n *Node) nodeIDForRaftAddr(addr string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for id, a := range n.knownPeers {
		if a == addr {
			return id
		}
	}
	return addr
}

// noteTerm announces TERM_CHANGED instantly when a higher term is observed.
// Shared by the transport wrapper (event-driven) and the broadcaster loop
// (safety net); guarded so it never double-announces.
func (n *Node) noteTerm(term uint64) {
	if term == 0 {
		return
	}

	n.mu.Lock()
	if term <= n.lastTerm {
		n.mu.Unlock()
		return
	}
	n.lastTerm = term
	n.mu.Unlock()

	n.hub.Broadcast(events.Event{
		Type:    events.EventTermChanged,
		NodeID:  n.cfg.NodeID,
		Term:    term,
		Message: fmt.Sprintf("Node %s observed Raft term %d", n.cfg.NodeID, term),
		Data:    map[string]any{"term": term},
	})
}

// IsLeader returns true if this node is currently the leader
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// RegisterPeerHTTP maps a node ID to its HTTP address
func (n *Node) RegisterPeerHTTP(nodeID, httpAddr string) {
	if httpAddr == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerHTTPAddrs[nodeID] = httpAddr
}

// httpAddrFor returns the mapped HTTP address or the port+1000 inference
// (caller must hold n.mu)
func (n *Node) httpAddrFor(nodeID, raftAddr string) string {
	if addr := n.peerHTTPAddrs[nodeID]; addr != "" {
		return addr
	}
	return inferHTTPAddrFromRaft(raftAddr)
}

// GetPeerHTTP returns the mapped HTTP address or infers it
func (n *Node) GetPeerHTTP(nodeID, raftAddr string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.httpAddrFor(nodeID, raftAddr)
}

func inferHTTPAddrFromRaft(raftAddr string) string {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return ""
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", host, port+1000)
}

// ApplyKV writes a key-value pair through Raft consensus
func (n *Node) ApplyKV(op fsm.OpType, key, value string, timeout time.Duration) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader (current leader: %s)", n.raft.Leader())
	}

	cmd := fsm.Command{
		Op:        op,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		ClientID:  n.cfg.NodeID,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command to raft: %w", err)
	}

	n.hub.Broadcast(events.Event{
		Type:    events.EventLogReplicated,
		NodeID:  n.cfg.NodeID,
		Term:    n.raft.CurrentTerm(),
		Message: fmt.Sprintf("Leader %s replicated %s [%s=%s] to cluster quorum (Log Index %d)", n.cfg.NodeID, op, key, value, future.Index()),
		Data: map[string]any{
			"index": future.Index(),
			"term":  n.raft.CurrentTerm(),
			"op":    op,
			"key":   key,
			"value": value,
		},
	})

	return nil
}

// AddVoter adds a new server as a voting member to the Raft cluster
func (n *Node) AddVoter(nodeID string, raftAddr string, httpAddr string) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	n.RegisterPeerHTTP(nodeID, httpAddr)

	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter %s (%s): %w", nodeID, raftAddr, err)
	}

	return nil
}

// RemoveServer removes a server from the Raft cluster
func (n *Node) RemoveServer(nodeID string) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	future := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server %s: %w", nodeID, err)
	}

	n.mu.Lock()
	delete(n.peerHealth, nodeID)
	n.mu.Unlock()

	return nil
}

// SetPeerHealth manually sets the health state for a peer node and broadcasts updates
func (n *Node) SetPeerHealth(nodeID string, healthy bool) {
	n.mu.Lock()
	n.peerHealth[nodeID] = healthy
	n.mu.Unlock()

	status := "offline"
	if healthy {
		status = "online"
	}
	n.hub.Broadcast(events.Event{
		Type:    events.EventNodeStatusChanged,
		NodeID:  nodeID,
		Term:    n.raft.CurrentTerm(),
		Message: fmt.Sprintf("Node %s is %s", nodeID, status),
		Data:    map[string]any{"node_id": nodeID, "healthy": healthy},
	})

	n.hub.Broadcast(events.Event{
		Type:    events.EventStateSnapshot,
		NodeID:  n.cfg.NodeID,
		Term:    n.raft.CurrentTerm(),
		Message: "State sync",
		Data:    n.GetState(),
	})
}

// TransferLeadership transfers leadership to a specified node or another suitable node
func (n *Node) TransferLeadership(targetID string, targetAddr string) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	var future raft.Future
	if targetID != "" && targetAddr != "" {
		future = n.raft.LeadershipTransferToServer(raft.ServerID(targetID), raft.ServerAddress(targetAddr))
	} else {
		future = n.raft.LeadershipTransfer()
	}

	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to transfer leadership: %w", err)
	}

	// No synthetic event here: the real TimeoutNow RPC is observed by the
	// transport wrapper and emitted as LEADERSHIP_TRANSFER on both sides

	return nil
}

// GetState returns the current cluster state snapshot
func (n *Node) GetState() events.ClusterState {
	leaderAddr, leaderID := n.raft.LeaderWithID()
	state := n.raft.State()
	term := n.raft.CurrentTerm()

	configFuture := n.raft.GetConfiguration()
	var servers []raft.Server
	if err := configFuture.Error(); err == nil {
		servers = configFuture.Configuration().Servers
	}

	n.mu.RLock()
	peers := make([]events.PeerInfo, 0, len(servers))
	for _, s := range servers {
		sID := string(s.ID)
		sAddr := string(s.Address)
		isSelf := sID == n.cfg.NodeID
		isLeader := sID == string(leaderID) || (leaderID == "" && sAddr == string(leaderAddr))

		role := "Follower"
		if isLeader {
			role = "Leader"
		} else if isSelf && state == raft.Candidate {
			role = "Candidate"
		}

		healthy := true
		if h, exists := n.peerHealth[sID]; exists {
			healthy = h
		}

		peers = append(peers, events.PeerInfo{
			ID:       sID,
			RaftAddr: sAddr,
			HTTPAddr: n.httpAddrFor(sID, sAddr),
			Role:     role,
			Voter:    s.Suffrage == raft.Voter,
			IsLeader: isLeader,
			IsSelf:   isSelf,
			Healthy:  healthy,
		})
	}
	leaderHTTPAddr := n.httpAddrFor(string(leaderID), string(leaderAddr))
	n.mu.RUnlock()

	// Last log term, read from the persistent log store
	var lastLogTerm uint64
	if lastIdx, err := n.boltStore.LastIndex(); err == nil && lastIdx > 0 {
		var lastLog raft.Log
		if err := n.boltStore.GetLog(lastIdx, &lastLog); err == nil {
			lastLogTerm = lastLog.Term
		}
	}

	// Subjective liveness signal: ms since last contact with the leader
	// (-1 = never contacted; a leader reports its own last successful round)
	lastContactMS := int64(-1)
	if lc := n.raft.LastContact(); !lc.IsZero() {
		lastContactMS = time.Since(lc).Milliseconds()
	}

	return events.ClusterState{
		SelfID:         n.cfg.NodeID,
		SelfRole:       state.String(),
		RaftAddr:       n.cfg.RaftAddr,
		HTTPAddr:       n.cfg.HTTPAddr,
		LeaderID:       string(leaderID),
		LeaderRaftAddr: string(leaderAddr),
		LeaderHTTPAddr: leaderHTTPAddr,
		CurrentTerm:    term,
		VotedFor:       n.votedForFromStore(),
		CommitIndex:    n.raft.CommitIndex(),
		LastLogIndex:   n.raft.LastIndex(),
		LastLogTerm:    lastLogTerm,
		AppliedIndex:   n.raft.AppliedIndex(),
		LastContactMS:  lastContactMS,
		Peers:          peers,
		ActiveClients:  n.hub.SubscriberCount(),
		KVData:         n.fsm.GetAll(),
		RaftStats:      n.raft.Stats(),
		Timestamp:      time.Now(),
	}
}

// Close shuts down the Raft node
func (n *Node) Close() error {
	close(n.stopCh)
	if n.pollingRunning.Load() {
		n.onSSEDeactivated()
	}

	future := n.raft.Shutdown()
	if err := future.Error(); err != nil {
		log.Printf("[Raft] Error during shutdown: %v", err)
	}

	if n.transport != nil {
		_ = n.transport.Close()
	}
	if n.boltStore != nil {
		_ = n.boltStore.Close()
	}

	return nil
}
