package raftnode

import (
	"fmt"
	"log"
	"maps"
	"net"
	"time"

	"raft-algo/internal/events"

	"github.com/hashicorp/raft"
)

func peerJoinedEvent(sID, sAddr, httpAddr string, term uint64) events.Event {
	return events.Event{
		Type:    events.EventPeerJoined,
		NodeID:  sID,
		Term:    term,
		Message: fmt.Sprintf("Node %s joined cluster (Raft %s, HTTP %s)", sID, sAddr, httpAddr),
		Data:    map[string]any{"node_id": sID, "raft_addr": sAddr, "http_addr": httpAddr},
	}
}

func peerRemovedEvent(sID, sAddr string, term uint64) events.Event {
	return events.Event{
		Type:    events.EventPeerRemoved,
		NodeID:  sID,
		Term:    term,
		Message: fmt.Sprintf("Node %s (%s) was removed from Raft configuration", sID, sAddr),
		Data:    map[string]any{"node_id": sID, "raft_addr": sAddr},
	}
}

// handleObservations processes raw Raft events from the registered observer
func (n *Node) handleObservations() {
	for {
		select {
		case <-n.stopCh:
			return
		case obs, ok := <-n.obsCh:
			if !ok {
				return
			}

			switch d := obs.Data.(type) {
			case raft.LeaderObservation:
				leaderID := string(d.LeaderID)
				leaderAddr := string(d.LeaderAddr)
				if leaderAddr == "" {
					leaderAddr = string(d.Leader)
				}
				if leaderID != "" && leaderID != n.cfg.NodeID {
					n.hub.Broadcast(events.Event{
						Type:    events.EventLeaderChanged,
						NodeID:  n.cfg.NodeID,
						Term:    n.raft.CurrentTerm(),
						Message: fmt.Sprintf("Leader discovered -> %s (%s)", leaderID, leaderAddr),
						Data:    map[string]any{"leader_id": leaderID, "leader_addr": leaderAddr},
					})
				}

			case raft.PeerObservation:
				sID := string(d.Peer.ID)
				sAddr := string(d.Peer.Address)

				n.mu.Lock()
				if d.Removed {
					delete(n.knownPeers, sID)
				} else {
					n.knownPeers[sID] = sAddr
				}
				n.mu.Unlock()

				term := n.raft.CurrentTerm()
				if d.Removed {
					n.hub.Broadcast(peerRemovedEvent(sID, sAddr, term))
				} else {
					n.hub.Broadcast(peerJoinedEvent(sID, sAddr, n.GetPeerHTTP(sID, sAddr), term))
				}

			case raft.RaftState:
				term := n.raft.CurrentTerm()
				switch d {
				case raft.Candidate:
					n.hub.Broadcast(events.Event{
						Type:    events.EventElectionStarted,
						NodeID:  n.cfg.NodeID,
						Term:    term,
						Message: fmt.Sprintf("Node %s initiated election for Term %d (Follower -> Candidate)", n.cfg.NodeID, term),
						Data:    map[string]any{"state": "Candidate", "term": term},
					})
				case raft.Shutdown:
					n.hub.Broadcast(events.Event{
						Type:    events.EventNodeStatusChanged,
						NodeID:  n.cfg.NodeID,
						Term:    term,
						Message: fmt.Sprintf("Node %s is shutting down", n.cfg.NodeID),
						Data:    map[string]any{"state": "Shutdown"},
					})
				}

			case raft.FailedHeartbeatObservation:
				// The engine repeats this observation on EVERY failed heartbeat
				// while the peer stays down — broadcast only the transition into
				// the failed state; recovery is reported by ResumedHeartbeat.
				peerID := string(d.PeerID)
				n.mu.Lock()
				alreadyFailed := n.hbFailedPeers[peerID]
				n.hbFailedPeers[peerID] = true
				n.mu.Unlock()
				if alreadyFailed {
					continue
				}

				n.hub.Broadcast(events.Event{
					Type:    events.EventNodeStatusChanged,
					NodeID:  n.cfg.NodeID,
					Term:    n.raft.CurrentTerm(),
					Message: fmt.Sprintf("Heartbeat to %s failed (unreachable since %s)", peerID, d.LastContact.Format("15:04:05")),
					Data:    map[string]any{"node_id": peerID, "heartbeat_failed": true},
				})

			case raft.ResumedHeartbeatObservation:
				peerID := string(d.PeerID)
				n.mu.Lock()
				delete(n.hbFailedPeers, peerID)
				n.mu.Unlock()

				n.hub.Broadcast(events.Event{
					Type:    events.EventNodeStatusChanged,
					NodeID:  n.cfg.NodeID,
					Term:    n.raft.CurrentTerm(),
					Message: fmt.Sprintf("Heartbeat to %s recovered", peerID),
					Data:    map[string]any{"node_id": peerID, "heartbeat_resumed": true},
				})
			}
		}
	}
}

// watchLeaderCh monitors the leader channel from Raft
func (n *Node) watchLeaderCh() {
	leaderCh := n.raft.LeaderCh()
	for {
		select {
		case <-n.stopCh:
			return
		case isLeader, ok := <-leaderCh:
			if !ok {
				return
			}
			term := n.raft.CurrentTerm()
			if isLeader {
				log.Printf("[Raft] Node %s elected as LEADER for term %d", n.cfg.NodeID, term)
				n.hub.Broadcast(events.Event{
					Type:    events.EventLeaderChanged,
					NodeID:  n.cfg.NodeID,
					Term:    term,
					Message: fmt.Sprintf("Node %s elected as LEADER for Term %d (Quorum votes granted)", n.cfg.NodeID, term),
					Data:    map[string]any{"leader_id": n.cfg.NodeID, "leader_addr": n.cfg.RaftAddr},
				})
			} else {
				log.Printf("[Raft] Node %s stepped down to FOLLOWER (term %d)", n.cfg.NodeID, term)
				n.hub.Broadcast(events.Event{
					Type:    events.EventLeadershipLost,
					NodeID:  n.cfg.NodeID,
					Term:    term,
					Message: fmt.Sprintf("Leader %s stepped down to Follower (Term %d)", n.cfg.NodeID, term),
					Data:    map[string]any{"node_id": n.cfg.NodeID, "role": "Follower"},
				})
			}
		}
	}
}

// onSSEActivated starts the active polling/broadcasting loop when the first SSE client connects
func (n *Node) onSSEActivated() {
	if n.pollingRunning.Swap(true) {
		return // already running
	}

	log.Printf("[Hub] SSE activated on node %s (lazy streaming started)", n.cfg.NodeID)
	n.pollCancel = make(chan struct{})

	n.hub.Broadcast(events.Event{
		Type:    events.EventStateSnapshot,
		NodeID:  n.cfg.NodeID,
		Term:    n.raft.CurrentTerm(),
		Message: "Connected to Raft live event stream",
		Data:    n.GetState(),
	})

	go n.runActiveBroadcaster(n.pollCancel)
}

// onSSEDeactivated stops active polling/diffing when all SSE clients disconnect
func (n *Node) onSSEDeactivated() {
	if !n.pollingRunning.Swap(false) {
		return // not running
	}

	log.Printf("[Hub] All SSE clients disconnected from node %s (lazy streaming stopped)", n.cfg.NodeID)
	if n.pollCancel != nil {
		close(n.pollCancel)
		n.pollCancel = nil
	}
}

// runActiveBroadcaster executes periodic state snapshot sync while active.
// Heartbeats, votes, appends and transfers are NOT synthesized here: they are
// emitted as real events by the transport wrapper (transport_observer.go).
func (n *Node) runActiveBroadcaster(cancelCh chan struct{}) {
	snapshotTicker := time.NewTicker(2 * time.Second)
	termTicker := time.NewTicker(time.Second)
	defer snapshotTicker.Stop()
	defer termTicker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-cancelCh:
			return
		case <-snapshotTicker.C:
			n.hub.Broadcast(events.Event{
				Type:    events.EventStateSnapshot,
				NodeID:  n.cfg.NodeID,
				Term:    n.raft.CurrentTerm(),
				Message: "Periodic state sync",
				Data:    n.GetState(),
			})
		case <-termTicker.C:
			// Safety net only: event-driven term tracking lives in the transport wrapper
			n.noteTerm(n.raft.CurrentTerm())
		}
	}
}

// continuousHealthCheck runs background peer connectivity probes
func (n *Node) continuousHealthCheck() {
	ticker := time.NewTicker(600 * time.Millisecond)
	defer ticker.Stop()

	time.Sleep(500 * time.Millisecond) // initial probe after startup delay
	n.checkPeersHealth()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.checkPeersHealth()
		}
	}
}

// checkPeersHealth probes TCP connectivity to peers and diffs the Raft
// configuration membership against this node's subjective knownPeers view
func (n *Node) checkPeersHealth() {
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return
	}
	servers := configFuture.Configuration().Servers
	term := n.raft.CurrentTerm()

	current := make(map[string]string, len(servers))
	for _, s := range servers {
		current[string(s.ID)] = string(s.Address)
	}

	n.mu.Lock()
	var joined, removed []events.Event
	for sID, sAddr := range current {
		if _, ok := n.knownPeers[sID]; !ok {
			n.knownPeers[sID] = sAddr
			joined = append(joined, peerJoinedEvent(sID, sAddr, n.httpAddrFor(sID, sAddr), term))
		}
	}
	for sID, sAddr := range n.knownPeers {
		if _, ok := current[sID]; !ok && sID != n.cfg.NodeID {
			delete(n.knownPeers, sID)
			removed = append(removed, peerRemovedEvent(sID, sAddr, term))
		}
	}
	oldHealth := maps.Clone(n.peerHealth)
	n.mu.Unlock()

	for _, evt := range joined {
		n.hub.Broadcast(evt)
	}
	for _, evt := range removed {
		n.hub.Broadcast(evt)
	}

	healthMap := make(map[string]bool, len(servers))
	changed := false
	for _, s := range servers {
		sID := string(s.ID)
		if sID == n.cfg.NodeID {
			healthMap[sID] = true
			continue
		}

		conn, err := net.DialTimeout("tcp", string(s.Address), 250*time.Millisecond)
		healthy := err == nil
		if healthy {
			_ = conn.Close()
		}

		healthMap[sID] = healthy

		if oldHealth[sID] != healthy {
			changed = true
			status := "OFFLINE (Unreachable)"
			if healthy {
				status = "ONLINE (Healthy)"
			}
			log.Printf("[Health] Node %s is %s", sID, status)
		}
	}

	n.mu.Lock()
	n.peerHealth = healthMap
	n.mu.Unlock()

	if changed {
		n.hub.Broadcast(events.Event{
			Type:    events.EventNodeStatusChanged,
			NodeID:  n.cfg.NodeID,
			Term:    n.raft.CurrentTerm(),
			Message: "Cluster peer health status updated",
			Data:    map[string]any{"health": healthMap},
		})

		n.hub.Broadcast(events.Event{
			Type:    events.EventStateSnapshot,
			NodeID:  n.cfg.NodeID,
			Term:    n.raft.CurrentTerm(),
			Message: "Cluster health changed",
			Data:    n.GetState(),
		})
	}
}
