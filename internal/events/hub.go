package events

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// hbFlushInterval is the coalescing window for high-frequency heartbeat events.
const hbFlushInterval = time.Second

// History cache sizes (lifecycle events vs. coalesced heartbeat batches).
const (
	maxHistory   = 50
	maxHBHistory = 10
)

// Hub manages SSE clients with lazy broadcasting and dual in-memory event history caches
type Hub struct {
	mu               sync.RWMutex
	clients          map[chan Event]struct{}
	onActivate       func()
	onDeactivate     func()
	sequence         atomic.Int64
	history          []Event
	heartbeatHistory []Event

	hbMu   sync.Mutex
	hbSent *hbAccum // outgoing heartbeats (this node is leader)
	hbRecv *hbAccum // incoming heartbeats (this node is follower)
}

// hbAccum coalesces high-frequency heartbeat RPCs into a single batch event
type hbAccum struct {
	count      int
	nodeID     string
	peerIDs    map[string]struct{}
	lastTerm   uint64
	lastCommit uint64
}

func newHBAccum() *hbAccum {
	return &hbAccum{peerIDs: make(map[string]struct{})}
}

// NewHub creates a new lazy event hub with dual ring buffer caches
func NewHub(onActivate func(), onDeactivate func()) *Hub {
	h := &Hub{
		clients:          make(map[chan Event]struct{}),
		onActivate:       onActivate,
		onDeactivate:     onDeactivate,
		history:          make([]Event, 0, maxHistory),
		heartbeatHistory: make([]Event, 0, maxHBHistory),
		hbSent:           newHBAccum(),
		hbRecv:           newHBAccum(),
	}
	go h.hbFlushLoop()
	return h
}

// hbFlushLoop periodically drains the heartbeat accumulators into batch events
func (h *Hub) hbFlushLoop() {
	ticker := time.NewTicker(hbFlushInterval)
	defer ticker.Stop()
	for range ticker.C {
		h.hbFlushOnce()
	}
}

func (h *Hub) hbFlushOnce() {
	h.hbMu.Lock()
	sent := h.hbSent
	recv := h.hbRecv
	if sent.count == 0 && recv.count == 0 {
		h.hbMu.Unlock()
		return
	}
	h.hbSent = newHBAccum()
	h.hbRecv = newHBAccum()
	h.hbMu.Unlock()

	if sent.count > 0 {
		h.broadcastDirect(sent.batchEvent(EventHeartbeatSent))
	}
	if recv.count > 0 {
		h.broadcastDirect(recv.batchEvent(EventHeartbeatReceived))
	}
}

// batchEvent renders an accumulator as a single coalesced heartbeat event
func (a *hbAccum) batchEvent(t EventType) Event {
	ids := make([]string, 0, len(a.peerIDs))
	for id := range a.peerIDs {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	data := map[string]any{
		"count":        a.count,
		"term":         a.lastTerm,
		"commit_index": a.lastCommit,
	}
	var msg string
	if t == EventHeartbeatSent {
		data["direction"] = "outgoing"
		data["role"] = "Leader"
		data["targets"] = ids
		msg = fmt.Sprintf("Leader dispatched %d heartbeat(s) (AppendEntries) to %s [Term %d]",
			a.count, strings.Join(ids, ", "), a.lastTerm)
	} else {
		data["direction"] = "incoming"
		data["role"] = "Follower"
		data["sources"] = ids
		msg = fmt.Sprintf("Follower received %d heartbeat(s) (AppendEntries) from %s [Term %d, Commit %d]",
			a.count, strings.Join(ids, ", "), a.lastTerm, a.lastCommit)
	}

	return Event{
		NodeID:  a.nodeID,
		Type:    t,
		Term:    a.lastTerm,
		Message: msg,
		Data:    data,
	}
}

// Subscribe registers a new SSE client and returns a receive channel and an unsubscribe func
func (h *Hub) Subscribe() (chan Event, func()) {
	ch := make(chan Event, 64)

	h.mu.Lock()
	h.clients[ch] = struct{}{}
	activate := len(h.clients) == 1
	h.mu.Unlock()

	if activate && h.onActivate != nil {
		go h.onActivate()
	}

	unsubscribe := func() {
		h.mu.Lock()
		if _, ok := h.clients[ch]; !ok {
			h.mu.Unlock()
			return
		}
		delete(h.clients, ch)
		close(ch)
		deactivate := len(h.clients) == 0
		h.mu.Unlock()

		if deactivate && h.onDeactivate != nil {
			go h.onDeactivate()
		}
	}

	return ch, unsubscribe
}

// IsActive returns true if there is at least one active subscriber
func (h *Hub) IsActive() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients) > 0
}

// SubscriberCount returns current active subscriber count
func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// GetHistory returns a thread-safe copy of the last maxHistory cached Raft events
func (h *Hub) GetHistory() []Event {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return slices.Clone(h.history)
}

// GetHeartbeatHistory returns a thread-safe copy of the last maxHBHistory cached heartbeat batches
func (h *Hub) GetHeartbeatHistory() []Event {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return slices.Clone(h.heartbeatHistory)
}

// Broadcast sends an event to all connected clients. High-frequency heartbeat
// events are coalesced into batch events flushed every hbFlushInterval.
func (h *Hub) Broadcast(evt Event) {
	if evt.Type == EventHeartbeatSent || evt.Type == EventHeartbeatReceived {
		if evt.Data != nil {
			h.accumulateHeartbeat(evt)
		}
		return
	}

	h.broadcastDirect(evt)
}

// accumulateHeartbeat folds a single heartbeat RPC event into the batch accumulators
func (h *Hub) accumulateHeartbeat(evt Event) {
	data, _ := evt.Data.(map[string]any)
	if data == nil {
		return
	}

	h.hbMu.Lock()
	defer h.hbMu.Unlock()

	accum := h.hbSent
	if evt.Type == EventHeartbeatReceived {
		accum = h.hbRecv
	}

	accum.count++
	if evt.NodeID != "" {
		accum.nodeID = evt.NodeID
	}
	if peerID, ok := data["peer_id"].(string); ok && peerID != "" {
		accum.peerIDs[peerID] = struct{}{}
	}
	if t, ok := data["term"].(uint64); ok {
		accum.lastTerm = t
	}
	if c, ok := data["commit_index"].(uint64); ok {
		accum.lastCommit = c
	}
}

// trimToLast keeps the newest max elements of s
func trimToLast[T any](s []T, max int) []T {
	if len(s) > max {
		return s[len(s)-max:]
	}
	return s
}

// broadcastDirect delivers an event to all connected clients and records it
// into the appropriate history cache (heartbeats go to the batch cache).
func (h *Hub) broadcastDirect(evt Event) {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	if evt.ID == "" {
		evt.ID = fmt.Sprintf("evt-%d-%d", time.Now().UnixMilli(), h.sequence.Add(1))
	}

	h.mu.Lock()
	switch {
	case evt.Type == EventHeartbeatSent || evt.Type == EventHeartbeatReceived:
		h.heartbeatHistory = trimToLast(append(h.heartbeatHistory, evt), maxHBHistory)
	case evt.Type != EventStateSnapshot:
		h.history = trimToLast(append(h.history, evt), maxHistory)
	}
	h.mu.Unlock()

	if !h.IsActive() {
		return // lazy broadcasting: no clients listening
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.clients {
		select {
		case ch <- evt:
		default:
			// non-blocking drop: a slow SSE client must never block the raft engine
			log.Printf("[Hub] Client channel full, dropping event: %s", evt.Type)
		}
	}
}
