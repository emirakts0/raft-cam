package raftnode

import (
	"fmt"
	"io"
	"sync"

	"raft-algo/internal/events"

	"github.com/hashicorp/raft"
)

// eventTransport wraps a raft.Transport and emits REAL RPC-level events
// (heartbeats, votes, appends, snapshots, leadership transfer) into the node's
// event hub — nothing the UI visualizes is synthesized here. Every RPC carries
// a term; a term bump is announced instantly (TERM_CHANGED) instead of being
// discovered by polling.
type eventTransport struct {
	inner raft.Transport
	node  *Node

	consumeOnce sync.Once
	wrappedCh   chan raft.RPC
}

// newEventTransport wraps the given raw transport with RPC event emission
func newEventTransport(inner raft.Transport, n *Node) *eventTransport {
	return &eventTransport{inner: inner, node: n}
}

// emit broadcasts a transport-level event on behalf of the local node
func (t *eventTransport) emit(evt events.Event) {
	if evt.NodeID == "" {
		evt.NodeID = t.node.ID()
	}
	if evt.Term == 0 && t.node.raft != nil {
		evt.Term = t.node.raft.CurrentTerm()
	}
	t.node.hub.Broadcast(evt)
}

// noteTerm announces TERM_CHANGED immediately when an RPC reveals a newer term
func (t *eventTransport) noteTerm(term uint64) {
	t.node.noteTerm(term)
}

// peerIDFromAddr resolves a Raft address (e.g. 127.0.0.1:7003) to a node ID
// using this node's subjective knownPeers view
func (t *eventTransport) peerIDFromAddr(addr raft.ServerAddress) string {
	return t.node.nodeIDForRaftAddr(string(addr))
}

// isMaintenanceRequest reports whether an AppendEntries carries no log entries.
// Raft reuses empty AEs for heartbeats, probes AND commit-index propagation —
// routine maintenance traffic that stays in the coalesced HEARTBEAT channel,
// never in the per-RPC APPEND_ENTRIES event stream reserved for real log data.
func isMaintenanceRequest(args *raft.AppendEntriesRequest) bool {
	return len(args.Entries) == 0
}

// ---- raft.Transport: outgoing RPCs ----------------------------------------

func (t *eventTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	term := args.Term
	t.noteTerm(term)
	peerID := string(id)

	if isMaintenanceRequest(args) {
		// Empty AE: heartbeat, probe or commit-index propagation — coalesced
		t.emit(events.Event{
			Type:    events.EventHeartbeatSent,
			Term:    term,
			Message: fmt.Sprintf("Heartbeat (AppendEntries) sent to %s [Term %d]", peerID, term),
			Data: map[string]any{
				"peer_id":      peerID,
				"term":         term,
				"commit_index": args.LeaderCommitIndex,
				"direction":    "outgoing",
			},
		})
	} else {
		t.emit(events.Event{
			Type:    events.EventAppendEntriesSent,
			Term:    term,
			Message: fmt.Sprintf("AppendEntries sent to %s with %d entr(y/ies) [Term %d, PrevLog %d]", peerID, len(args.Entries), term, args.PrevLogEntry),
			Data: map[string]any{
				"peer_id":   peerID,
				"term":      term,
				"entries":   len(args.Entries),
				"prev_log":  args.PrevLogEntry,
				"direction": "outgoing",
			},
		})
	}

	err := t.inner.AppendEntries(id, target, args, resp)

	if err == nil && !isMaintenanceRequest(args) && resp != nil {
		outcome := "replicated"
		if !resp.Success {
			outcome = "rejected (log mismatch)"
		}
		t.emit(events.Event{
			Type:    events.EventAppendEntriesResult,
			Term:    term,
			Message: fmt.Sprintf("AppendEntries to %s %s [Term %d, FollowerLastLog %d]", peerID, outcome, resp.Term, resp.LastLog),
			Data: map[string]any{
				"peer_id":   peerID,
				"success":   resp.Success,
				"term":      resp.Term,
				"last_log":  resp.LastLog,
				"direction": "outgoing",
			},
		})
	}

	return err
}

func (t *eventTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	t.noteTerm(args.Term)
	t.emit(events.Event{
		Type:    events.EventVoteRequested,
		Term:    args.Term,
		Message: fmt.Sprintf("Vote requested from %s for Term %d (last log %d@%d)%s", string(id), args.Term, args.LastLogIndex, args.LastLogTerm, voteSuffix(args.LeadershipTransfer)),
		Data: map[string]any{
			"target_id":      string(id),
			"term":           args.Term,
			"last_log_index": args.LastLogIndex,
			"last_log_term":  args.LastLogTerm,
			"transfer":       args.LeadershipTransfer,
			"direction":      "outgoing",
		},
	})

	err := t.inner.RequestVote(id, target, args, resp)

	if err == nil && resp != nil {
		t.emitVoteResult(string(id), args.Term, resp.Granted, false, false)
	}

	return err
}

func (t *eventTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	t.noteTerm(args.Term)
	t.emit(events.Event{
		Type:    events.EventSnapshotInstall,
		Term:    args.Term,
		Message: fmt.Sprintf("Snapshot (index %d, term %d) streaming to %s", args.LastLogIndex, args.LastLogTerm, string(id)),
		Data: map[string]any{
			"target_id":      string(id),
			"term":           args.Term,
			"last_log_index": args.LastLogIndex,
			"last_log_term":  args.LastLogTerm,
			"size":           args.Size,
			"direction":      "outgoing",
		},
	})
	return t.inner.InstallSnapshot(id, target, args, resp, data)
}

func (t *eventTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	t.emit(events.Event{
		Type:    events.EventLeadershipTransfer,
		Message: fmt.Sprintf("Leadership transfer (TimeoutNow) sent to %s", string(id)),
		Data: map[string]any{
			"target_id": string(id),
			"direction": "outgoing",
		},
	})
	return t.inner.TimeoutNow(id, target, args, resp)
}

// ---- raft.Transport: pipeline (ongoing replication) ------------------------

func (t *eventTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	inner, err := t.inner.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	return &eventPipeline{inner: inner, t: t, peerID: string(id)}, nil
}

// eventPipeline wraps an AppendPipeline to observe pipelined replication
type eventPipeline struct {
	inner  raft.AppendPipeline
	t      *eventTransport
	peerID string

	consumeOnce sync.Once
	wrappedCh   chan raft.AppendFuture
}

func (p *eventPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	if len(args.Entries) > 0 {
		p.t.noteTerm(args.Term)
		p.t.emit(events.Event{
			Type:    events.EventAppendEntriesSent,
			Term:    args.Term,
			Message: fmt.Sprintf("AppendEntries pipelined to %s with %d entr(y/ies) [Term %d]", p.peerID, len(args.Entries), args.Term),
			Data: map[string]any{
				"peer_id":   p.peerID,
				"term":      args.Term,
				"entries":   len(args.Entries),
				"pipeline":  true,
				"direction": "outgoing",
			},
		})
	}
	return p.inner.AppendEntries(args, resp)
}

func (p *eventPipeline) Consumer() <-chan raft.AppendFuture {
	p.consumeOnce.Do(func() {
		p.wrappedCh = make(chan raft.AppendFuture)
		go func() {
			for future := range p.inner.Consumer() {
				// Tee: watch the future asynchronously for its result and
				// forward it immediately so raft's pipeline loop never blocks.
				go p.watchFuture(future)
				p.wrappedCh <- future
			}
			close(p.wrappedCh)
		}()
	})
	return p.wrappedCh
}

func (p *eventPipeline) watchFuture(future raft.AppendFuture) {
	if err := future.Error(); err != nil {
		return // transport-level failure; raft logs and retries
	}
	req := future.Request()
	resp := future.Response()
	if req == nil || resp == nil || len(req.Entries) == 0 {
		return
	}
	outcome := "replicated"
	if !resp.Success {
		outcome = "rejected (log mismatch)"
	}
	p.t.emit(events.Event{
		Type:    events.EventAppendEntriesResult,
		Term:    resp.Term,
		Message: fmt.Sprintf("AppendEntries to %s %s [Term %d]", p.peerID, outcome, resp.Term),
		Data: map[string]any{
			"peer_id":   p.peerID,
			"success":   resp.Success,
			"term":      resp.Term,
			"pipeline":  true,
			"direction": "outgoing",
		},
	})
}

func (p *eventPipeline) Close() error {
	return p.inner.Close()
}

// ---- raft.Transport: incoming RPCs ----------------------------------------

// SetHeartbeatHandler wraps the heartbeat fast-path so that every REAL
// heartbeat received from the leader emits an event before processing
func (t *eventTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	if cb == nil {
		t.inner.SetHeartbeatHandler(nil)
		return
	}
	t.inner.SetHeartbeatHandler(func(rpc raft.RPC) {
		if req, ok := rpc.Command.(*raft.AppendEntriesRequest); ok {
			t.noteTerm(req.Term)
			t.emit(events.Event{
				Type:    events.EventHeartbeatReceived,
				Term:    req.Term,
				Message: fmt.Sprintf("Heartbeat (AppendEntries) received from leader %s [Term %d]", t.leaderIDFor(req), req.Term),
				Data: map[string]any{
					"peer_id":      t.leaderIDFor(req),
					"term":         req.Term,
					"commit_index": req.LeaderCommitIndex,
					"direction":    "incoming",
				},
			})
		}
		cb(rpc)
	})
}

// leaderIDFor resolves the leader identity from an incoming AppendEntries request
func (t *eventTransport) leaderIDFor(req *raft.AppendEntriesRequest) string {
	addr := req.RPCHeader.Addr
	if len(addr) == 0 {
		addr = req.Leader
	}
	if len(addr) == 0 {
		return "unknown"
	}
	decoded := t.inner.DecodePeer(addr)
	return t.peerIDFromAddr(decoded)
}

// Consumer returns a wrapped channel that observes incoming RPCs (votes,
// timeouts, snapshots, non-heartbeat appends) and forwards them to raft
func (t *eventTransport) Consumer() <-chan raft.RPC {
	t.consumeOnce.Do(func() {
		t.wrappedCh = make(chan raft.RPC)
		go t.forwardConsumer()
	})
	return t.wrappedCh
}

func (t *eventTransport) forwardConsumer() {
	for rpc := range t.inner.Consumer() {
		switch cmd := rpc.Command.(type) {
		case *raft.RequestVoteRequest:
			t.onIncomingVoteRequest(rpc, cmd)
			continue // forwarded with a wrapped response channel

		case *raft.RequestPreVoteRequest:
			t.onIncomingPreVoteRequest(rpc, cmd)
			continue // forwarded with a wrapped response channel

		case *raft.TimeoutNowRequest:
			t.emit(events.Event{
				Type:    events.EventLeadershipTransfer,
				Message: "Leadership transfer (TimeoutNow) received from leader -> starting election",
				Data: map[string]any{
					"target_id": t.node.ID(),
					"direction": "incoming",
				},
			})

		case *raft.InstallSnapshotRequest:
			t.noteTerm(cmd.Term)
			t.emit(events.Event{
				Type:    events.EventSnapshotInstall,
				Term:    cmd.Term,
				Message: fmt.Sprintf("Snapshot (index %d, term %d) received from leader", cmd.LastLogIndex, cmd.LastLogTerm),
				Data: map[string]any{
					"term":           cmd.Term,
					"last_log_index": cmd.LastLogIndex,
					"last_log_term":  cmd.LastLogTerm,
					"size":           cmd.Size,
					"direction":      "incoming",
				},
			})

		case *raft.AppendEntriesRequest:
			// Heartbeats take the fast-path once raft registers the handler
			// (HEARTBEAT_RECEIVED above). Empty AEs that still land here are
			// commit-index propagation — routine maintenance, term-tracked
			// only. Only a real log-carrying append becomes an event.
			t.noteTerm(cmd.Term)
			if len(cmd.Entries) > 0 {
				leader := t.leaderIDFor(cmd)
				t.emit(events.Event{
					Type:    events.EventAppendEntriesRecv,
					Term:    cmd.Term,
					Message: fmt.Sprintf("AppendEntries received from leader %s with %d entr(y/ies) [Term %d, PrevLog %d, Commit %d]", leader, len(cmd.Entries), cmd.Term, cmd.PrevLogEntry, cmd.LeaderCommitIndex),
					Data: map[string]any{
						"leader_id":    leader,
						"term":         cmd.Term,
						"entries":      len(cmd.Entries),
						"prev_log":     cmd.PrevLogEntry,
						"commit_index": cmd.LeaderCommitIndex,
						"direction":    "incoming",
					},
				})
			}
		}
		t.wrappedCh <- rpc
	}
	close(t.wrappedCh)
}

// onIncomingVoteRequest emits VOTE_REQUESTED and wraps the response channel so
// the vote this node casts (granted/rejected) is announced when raft responds
func (t *eventTransport) onIncomingVoteRequest(rpc raft.RPC, req *raft.RequestVoteRequest) {
	t.noteTerm(req.Term)
	candidateID := t.candidateIDFor(req.RPCHeader.Addr, req.Candidate)
	t.emit(events.Event{
		Type:    events.EventVoteRequested,
		Term:    req.Term,
		Message: fmt.Sprintf("Vote requested by %s for Term %d (last log %d@%d)%s", candidateID, req.Term, req.LastLogIndex, req.LastLogTerm, voteSuffix(req.LeadershipTransfer)),
		Data: map[string]any{
			"candidate_id":   candidateID,
			"term":           req.Term,
			"last_log_index": req.LastLogIndex,
			"last_log_term":  req.LastLogTerm,
			"transfer":       req.LeadershipTransfer,
			"direction":      "incoming",
		},
	})
	t.forwardWithVoteCapture(rpc, candidateID, req.Term, false)
}

// onIncomingPreVoteRequest is the pre-vote variant of onIncomingVoteRequest
func (t *eventTransport) onIncomingPreVoteRequest(rpc raft.RPC, req *raft.RequestPreVoteRequest) {
	t.noteTerm(req.Term)
	candidateID := t.candidateIDFor(req.RPCHeader.Addr, nil)
	t.emit(events.Event{
		Type:    events.EventVoteRequested,
		Term:    req.Term,
		Message: fmt.Sprintf("Pre-vote requested by %s for Term %d (last log %d@%d)", candidateID, req.Term, req.LastLogIndex, req.LastLogTerm),
		Data: map[string]any{
			"candidate_id":   candidateID,
			"term":           req.Term,
			"last_log_index": req.LastLogIndex,
			"last_log_term":  req.LastLogTerm,
			"pre_vote":       true,
			"direction":      "incoming",
		},
	})
	t.forwardWithVoteCapture(rpc, candidateID, req.Term, true)
}

// candidateIDFor resolves the requesting candidate's node ID from the RPC
// header address (preferred) or the deprecated Candidate field
func (t *eventTransport) candidateIDFor(headerAddr []byte, deprecated []byte) string {
	raw := headerAddr
	if len(raw) == 0 {
		raw = deprecated
	}
	if len(raw) == 0 {
		return "unknown"
	}
	return t.peerIDFromAddr(t.inner.DecodePeer(raw))
}

// forwardWithVoteCapture rewrites the RPC with a private response channel,
// observes the granted/rejected decision, then relays it to raft's channel
func (t *eventTransport) forwardWithVoteCapture(rpc raft.RPC, candidateID string, term uint64, preVote bool) {
	respCh := make(chan raft.RPCResponse, 1)
	go func() {
		resp := <-respCh
		if resp.Error == nil {
			if vr, ok := resp.Response.(*raft.RequestVoteResponse); ok {
				t.emitVoteResult(candidateID, vr.Term, vr.Granted, true, preVote)
			}
		}
		rpc.RespChan <- resp
	}()
	t.wrappedCh <- raft.RPC{
		Command:  rpc.Command,
		Reader:   rpc.Reader,
		RespChan: respCh,
	}
}

// emitVoteResult announces a vote decision. thisNodeVoted=true means this node
// is the voter casting the vote; false means this node is the candidate that
// received the peer's decision. preVote marks PreVote-round RPCs so the UI can
// visually separate them from the binding election round.
func (t *eventTransport) emitVoteResult(peerID string, term uint64, granted bool, thisNodeVoted bool, preVote bool) {
	evtType := events.EventVoteRejected
	verb := "rejected"
	if granted {
		evtType = events.EventVoteGranted
		verb = "granted"
	}
	if preVote {
		verb = "pre-vote " + verb
	}
	var msg string
	data := map[string]any{
		"peer_id": peerID,
		"granted": granted,
		"term":    term,
	}
	if preVote {
		data["pre_vote"] = true
	}
	if thisNodeVoted {
		data["direction"] = "outgoing"
		data["candidate_id"] = peerID
		msg = fmt.Sprintf("Vote %s for %s (Term %d)", verb, peerID, term)
	} else {
		data["direction"] = "incoming"
		data["voter_id"] = peerID
		msg = fmt.Sprintf("%s %s our vote request (Term %d)", peerID, verb, term)
	}
	t.emit(events.Event{
		Type:    evtType,
		Term:    term,
		Message: msg,
		Data:    data,
	})
}

func voteSuffix(leadershipTransfer bool) string {
	if leadershipTransfer {
		return " [leadership transfer]"
	}
	return ""
}

// ---- raft.Transport: passthrough -------------------------------------------

func (t *eventTransport) LocalAddr() raft.ServerAddress {
	return t.inner.LocalAddr()
}

func (t *eventTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return t.inner.EncodePeer(id, addr)
}

func (t *eventTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return t.inner.DecodePeer(buf)
}

// Close stops the wrapper goroutines and closes the inner transport if supported
func (t *eventTransport) Close() error {
	if closer, ok := t.inner.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// ---- WithPreVote passthrough (keeps pre-vote enabled through the wrapper) --

var _ raft.WithPreVote = (*eventTransport)(nil)

func (t *eventTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	t.noteTerm(args.Term)
	t.emit(events.Event{
		Type:    events.EventVoteRequested,
		Term:    args.Term,
		Message: fmt.Sprintf("Pre-vote requested from %s for Term %d (last log %d@%d)", string(id), args.Term, args.LastLogIndex, args.LastLogTerm),
		Data: map[string]any{
			"target_id":      string(id),
			"term":           args.Term,
			"last_log_index": args.LastLogIndex,
			"last_log_term":  args.LastLogTerm,
			"pre_vote":       true,
			"direction":      "outgoing",
		},
	})

	inner, ok := t.inner.(raft.WithPreVote)
	if !ok {
		return fmt.Errorf("inner transport does not support pre-vote")
	}
	err := inner.RequestPreVote(id, target, args, resp)

	if err == nil && resp != nil {
		t.emitVoteResult(string(id), resp.Term, resp.Granted, false, true)
	}

	return err
}
