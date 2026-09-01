package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	"raft-algo/internal/events"
	"raft-algo/internal/fsm"
	"raft-algo/internal/processmgr"
	"raft-algo/internal/raftnode"
)

var httpClient = &http.Client{Timeout: 5 * time.Second}

// Server handles all HTTP and SSE API requests
type Server struct {
	node       *raftnode.Node
	procMgr    *processmgr.Manager
	webFS      fs.FS
	httpServer *http.Server
	mux        *http.ServeMux
}

// NewServer creates a new API server
func NewServer(node *raftnode.Node, procMgr *processmgr.Manager, webFS fs.FS) *Server {
	s := &Server{
		node:    node,
		procMgr: procMgr,
		webFS:   webFS,
		mux:     http.NewServeMux(),
	}

	s.routes()
	return s
}

// routes configures all HTTP endpoints (Go 1.22 method-aware patterns)
func (s *Server) routes() {
	// Static frontend
	if s.webFS != nil {
		s.mux.Handle("/", http.FileServer(http.FS(s.webFS)))
	} else {
		s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("Raft Node API Server"))
		})
	}

	// State & SSE
	s.mux.HandleFunc("GET /state", s.handleGetState)
	s.mux.HandleFunc("GET /events", s.handleSSEEvents)

	// Raft operations
	s.mux.HandleFunc("POST /join", s.handleJoin)
	s.mux.HandleFunc("POST /remove", s.handleRemove)
	s.mux.HandleFunc("POST /leader/transfer", s.handleLeaderTransfer)

	// FSM KV Store
	s.mux.HandleFunc("POST /fsm/set", s.handleFSMSet)
	s.mux.HandleFunc("POST /fsm/delete", s.handleFSMDelete)
	s.mux.HandleFunc("GET /fsm/data", s.handleFSMData)

	// Node management (dynamic cluster scaling)
	s.mux.HandleFunc("POST /nodes", s.handleSpawnNodes)
	s.mux.HandleFunc("POST /nodes/{id}/stop", s.handleStopNode)
	s.mux.HandleFunc("DELETE /nodes/{id}", s.handleRemoveNode)
}

// ServeHTTP implements http.Handler with CORS wrapper
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS, PUT")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	s.mux.ServeHTTP(w, r)
}

// ListenAndServe starts the HTTP server
func (s *Server) ListenAndServe(addr string) error {
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s,
	}
	log.Printf("[HTTP] Server listening on http://%s", addr)
	return s.httpServer.ListenAndServe()
}

// Close stops the HTTP server
func (s *Server) Close() error {
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// decodeBody parses the request body into v; returns false and answers the
// client with 400 when the body is not valid JSON.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	return true
}

// forwardToLeader proxies the request to the current leader and answers the
// client with the leader's response. Returns false when the leader is unknown.
func (s *Server) forwardToLeader(w http.ResponseWriter, path string, payload any) bool {
	state := s.node.GetState()
	if state.LeaderHTTPAddr == "" {
		return false
	}
	proxyJSON(w, http.MethodPost, "http://"+state.LeaderHTTPAddr+path, payload)
	return true
}

// notLeader answers a request that cannot be forwarded because no leader is known
func (s *Server) notLeader(w http.ResponseWriter) {
	writeError(w, http.StatusServiceUnavailable, "not leader and leader unknown")
}

// handleGetState returns the cluster snapshot
func (s *Server) handleGetState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.node.GetState())
}

// writeSSE writes one SSE frame; the id line is included when the event has one
func writeSSE(w io.Writer, evt events.Event) {
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	if evt.ID != "" {
		fmt.Fprintf(w, "id: %s\n", evt.ID)
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, data)
}

// handleSSEEvents streams Raft events over Server-Sent Events
func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	eventCh, unsubscribe := s.node.Hub().Subscribe()
	defer unsubscribe()

	log.Printf("[SSE] Client connected from %s (active clients: %d)", r.RemoteAddr, s.node.Hub().SubscriberCount())

	// Initial full cluster state snapshot
	writeSSE(w, events.Event{Type: events.EventStateSnapshot, Data: s.node.GetState()})

	// Replay cached history (lifecycle events, then heartbeat batches)
	for _, evt := range s.node.Hub().GetHistory() {
		writeSSE(w, evt)
	}
	for _, evt := range s.node.Hub().GetHeartbeatHistory() {
		writeSSE(w, evt)
	}

	// Mark history sync complete so the client knows subsequent events are live
	writeSSE(w, events.Event{Type: events.EventSyncComplete, Data: map[string]any{}})
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			log.Printf("[SSE] Client disconnected from %s", r.RemoteAddr)
			return
		case evt, ok := <-eventCh:
			if !ok {
				return
			}
			writeSSE(w, evt)
			flusher.Flush()
		}
	}
}

// handleJoin adds a node to the cluster
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
		HTTPAddr string `json:"http_addr"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.NodeID == "" || req.RaftAddr == "" {
		writeError(w, http.StatusBadRequest, "node_id and raft_addr are required")
		return
	}

	if !s.node.IsLeader() {
		if s.forwardToLeader(w, "/join", req) {
			return
		}
		writeError(w, http.StatusServiceUnavailable, "leader unknown, retry shortly")
		return
	}

	if err := s.node.AddVoter(req.NodeID, req.RaftAddr, req.HTTPAddr); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Node %s joined cluster", req.NodeID),
	})
}

// handleRemove removes a node from the cluster
func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"node_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.NodeID == "" {
		writeError(w, http.StatusBadRequest, "node_id is required")
		return
	}

	if !s.node.IsLeader() {
		if s.forwardToLeader(w, "/remove", req) {
			return
		}
		s.notLeader(w)
		return
	}

	if err := s.node.RemoveServer(req.NodeID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Node %s removed from cluster", req.NodeID),
	})
}

// handleLeaderTransfer transfers leadership
func (s *Server) handleLeaderTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	if !s.node.IsLeader() {
		if s.forwardToLeader(w, "/leader/transfer", req) {
			return
		}
		writeError(w, http.StatusServiceUnavailable, "not leader")
		return
	}

	if err := s.node.TransferLeadership(req.NodeID, req.RaftAddr); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Leadership transfer initiated",
	})
}

// handleFSMSet writes a key-value pair through Raft consensus
func (s *Server) handleFSMSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	if !s.node.IsLeader() {
		state := s.node.GetState()
		if state.LeaderHTTPAddr == "" {
			s.notLeader(w)
			return
		}
		s.node.Hub().Broadcast(events.Event{
			Type:    events.EventProposalForwarded,
			NodeID:  s.node.ID(),
			Term:    state.CurrentTerm,
			Message: fmt.Sprintf("Follower %s forwarding write proposal [%s=%s] to Leader %s (%s)", s.node.ID(), req.Key, req.Value, state.LeaderID, state.LeaderHTTPAddr),
			Data: map[string]any{
				"from_node": s.node.ID(),
				"to_node":   state.LeaderID,
				"key":       req.Key,
				"value":     req.Value,
			},
		})
		proxyJSON(w, http.MethodPost, "http://"+state.LeaderHTTPAddr+"/fsm/set", req)
		return
	}

	s.node.Hub().Broadcast(events.Event{
		Type:    events.EventProposalReceived,
		NodeID:  s.node.ID(),
		Term:    s.node.GetState().CurrentTerm,
		Message: fmt.Sprintf("Leader %s received write proposal [%s=%s], proposing to Raft log...", s.node.ID(), req.Key, req.Value),
		Data:    map[string]any{"key": req.Key, "value": req.Value},
	})

	if err := s.node.ApplyKV(fsm.OpSet, req.Key, req.Value, 5*time.Second); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"key":     req.Key,
		"value":   req.Value,
	})
}

// handleFSMDelete deletes a key through Raft consensus
func (s *Server) handleFSMDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if !decodeBody(w, r, &req) || req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	if !s.node.IsLeader() {
		if s.forwardToLeader(w, "/fsm/delete", req) {
			return
		}
		s.notLeader(w)
		return
	}

	if err := s.node.ApplyKV(fsm.OpDelete, req.Key, "", 5*time.Second); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"key":     req.Key,
	})
}

// handleFSMData returns all data in the FSM
func (s *Server) handleFSMData(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"data":    s.node.FSM().GetAll(),
		"history": s.node.FSM().GetHistory(),
	})
}

// handleSpawnNodes spawns new node processes (POST /nodes?count=N or {"count": N})
func (s *Server) handleSpawnNodes(w http.ResponseWriter, r *http.Request) {
	count := 1
	var req struct {
		Count int `json:"count"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Count > 0 {
		count = req.Count
	}
	count = min(count, 50) // cap batch size

	state := s.node.GetState()
	joinAddr := "http://" + s.node.HTTPAddr()
	if state.LeaderHTTPAddr != "" {
		joinAddr = "http://" + state.LeaderHTTPAddr
	}

	existingPeers := make([]string, len(state.Peers))
	for i, p := range state.Peers {
		existingPeers[i] = p.ID
	}

	spawned := make([]map[string]any, 0, count)
	for range count {
		newNodeID := s.procMgr.NextAvailableNodeID(existingPeers)
		existingPeers = append(existingPeers, newNodeID)

		procInfo, err := s.procMgr.SpawnNode(newNodeID, joinAddr)
		if err != nil {
			log.Printf("[API] Error spawning node %s: %v", newNodeID, err)
			continue
		}

		spawned = append(spawned, map[string]any{
			"node_id":   procInfo.NodeID,
			"raft_addr": procInfo.RaftAddr,
			"http_addr": procInfo.HTTPAddr,
			"pid":       procInfo.PID,
		})

		time.Sleep(100 * time.Millisecond) // brief stagger between spawns
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Spawned %d node(s)", len(spawned)),
		"nodes":   spawned,
	})
}

// handleStopNode terminates a node process without removing it from the cluster
func (s *Server) handleStopNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")

	s.node.SetPeerHealth(nodeID, false)
	_ = s.procMgr.StopProcess(nodeID)

	if nodeID == s.node.ID() {
		go func() {
			time.Sleep(200 * time.Millisecond)
			os.Exit(0)
		}()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Node process %s stopped (offline)", nodeID),
	})
}

// handleRemoveNode removes a node from the cluster and terminates its process
func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")

	if s.node.IsLeader() {
		if err := s.node.RemoveServer(nodeID); err != nil {
			log.Printf("[API] Error removing server %s from raft: %v", nodeID, err)
		}
	} else if state := s.node.GetState(); state.LeaderHTTPAddr != "" {
		_ = requestRemove(state.LeaderHTTPAddr, nodeID)
	}

	_ = s.procMgr.StopProcess(nodeID)

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Node %s removed and stopped", nodeID),
	})
}

// requestRemove asks the leader to remove a node (fire-and-forget)
func requestRemove(leaderHTTP, nodeID string) error {
	payload, _ := json.Marshal(map[string]string{"node_id": nodeID})
	resp, err := httpClient.Post(fmt.Sprintf("http://%s/remove", leaderHTTP), "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// proxyJSON forwards a JSON request to another node and relays its response
func proxyJSON(w http.ResponseWriter, method, targetURL string, data any) {
	body, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "failed to serialize proxy body", http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequest(method, targetURL, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("proxy to leader failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
