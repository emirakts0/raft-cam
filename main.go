package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"raft-algo/internal/api"
	"raft-algo/internal/processmgr"
	"raft-algo/internal/raftnode"
	"raft-algo/web"
)

func main() {
	nodeID := flag.String("node-id", "node-1", "Unique identifier for this Raft node")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7001", "Raft internal TCP communication address")
	httpAddr := flag.String("http-addr", "127.0.0.1:8001", "HTTP / SSE API and Web UI address")
	dataDir := flag.String("data-dir", "", "Directory for Raft logs and snapshots (default: data/<node-id>)")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the cluster with this node as initial member")
	joinAddr := flag.String("join-addr", "", "Target HTTP address of an existing node in the cluster to join (e.g. http://127.0.0.1:8001)")

	flag.Parse()

	log.Printf("Starting Raft node %s (raft %s, http %s, bootstrap=%v, join=%s)",
		*nodeID, *raftAddr, *httpAddr, *bootstrap, *joinAddr)

	node, err := raftnode.NewNode(raftnode.Config{
		NodeID:    *nodeID,
		RaftAddr:  *raftAddr,
		HTTPAddr:  *httpAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
		JoinAddr:  *joinAddr,
	})
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Raft node: %v", err)
	}

	procMgr := processmgr.NewManager(*nodeID, *raftAddr, *httpAddr)
	server := api.NewServer(node, procMgr, web.GetDistFS())

	go func() {
		if err := server.ListenAndServe(*httpAddr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	if *joinAddr != "" && !*bootstrap {
		go joinCluster(normalizeJoinAddr(*joinAddr), *nodeID, *raftAddr, *httpAddr)
	}

	// Graceful shutdown on SIGINT / SIGTERM (SIGHUP ignored: terminal disconnect)
	signal.Ignore(syscall.SIGHUP)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	log.Printf("[Shutdown] Received signal %v, shutting down node %s...", sig, *nodeID)

	procMgr.StopAll()
	_ = server.Close()
	_ = node.Close()

	log.Printf("[Shutdown] Node %s exited cleanly.", *nodeID)
}

func normalizeJoinAddr(addr string) string {
	addr = strings.TrimPrefix(strings.TrimPrefix(addr, "http://"), "https://")
	return strings.TrimRight(addr, "/")
}

// joinCluster retries the join request until the target accepts it
func joinCluster(targetHTTP, nodeID, raftAddr, httpAddr string) {
	const maxAttempts = 15

	time.Sleep(500 * time.Millisecond) // let the local HTTP server come up first
	log.Printf("[Join] Attempting to join cluster via %s...", targetHTTP)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := processmgr.RequestJoin(targetHTTP, nodeID, raftAddr, httpAddr)
		if err == nil {
			log.Printf("[Join] Successfully joined cluster via %s!", targetHTTP)
			return
		}
		log.Printf("[Join] Attempt %d/%d failed (%v), retrying in 1s...", attempt, maxAttempts, err)
		time.Sleep(time.Second)
	}
	log.Printf("[WARN] Could not join cluster after %d attempts", maxAttempts)
}
