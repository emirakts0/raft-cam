package processmgr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// ProcessInfo holds information about a spawned child node process
type ProcessInfo struct {
	NodeID    string    `json:"node_id"`
	RaftAddr  string    `json:"raft_addr"`
	HTTPAddr  string    `json:"http_addr"`
	DataDir   string    `json:"data_dir"`
	PID       int       `json:"pid"`
	StartTime time.Time `json:"start_time"`
	cmd       *exec.Cmd
}

// Manager orchestrates child node processes
type Manager struct {
	mu          sync.Mutex
	children    map[string]*ProcessInfo
	localNodeID string
	localHTTP   string
	localRaft   string
	executable  string
	nextPort    int
}

// NewManager creates a new process manager
func NewManager(localNodeID, localRaft, localHTTP string) *Manager {
	exe, err := os.Executable()
	if err != nil {
		exe = "./bin/raft-node"
	}

	_ = os.MkdirAll("logs", 0755)
	_ = os.MkdirAll("data", 0755)

	return &Manager{
		children:    make(map[string]*ProcessInfo),
		localNodeID: localNodeID,
		localHTTP:   localHTTP,
		localRaft:   localRaft,
		executable:  exe,
		nextPort:    7004,
	}
}

// FindFreePort finds an available TCP port starting from startPort
func FindFreePort(startPort int) (int, error) {
	for port := startPort; port < startPort+2000; port++ {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			_ = listener.Close()
			return port, nil
		}
	}
	return 0, fmt.Errorf("no free port available in range starting at %d", startPort)
}

// NextAvailableNodeID generates a unique node ID
func (m *Manager) NextAvailableNodeID(existingPeers []string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	used := make(map[string]bool)
	used[m.localNodeID] = true
	for _, p := range existingPeers {
		used[p] = true
	}
	for id := range m.children {
		used[id] = true
	}

	for i := 1; i <= 500; i++ {
		if id := fmt.Sprintf("node-%d", i); !used[id] {
			return id
		}
	}
	return fmt.Sprintf("node-%d", time.Now().Unix()%10000)
}

// SpawnNode spawns a new node process and returns its configuration
func (m *Manager) SpawnNode(nodeID string, joinHTTPAddr string) (*ProcessInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if joinHTTPAddr == "" {
		joinHTTPAddr = fmt.Sprintf("http://%s", m.localHTTP)
	}

	// Allocate free ports
	raftPort, err := FindFreePort(m.nextPort)
	if err != nil {
		return nil, fmt.Errorf("failed to find raft port: %w", err)
	}
	m.nextPort = raftPort + 1

	httpPort, err := FindFreePort(raftPort + 1000)
	if err != nil {
		httpPort, err = FindFreePort(8004)
		if err != nil {
			return nil, fmt.Errorf("failed to find http port: %w", err)
		}
	}

	raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)
	dataDir := filepath.Join("data", nodeID)

	_ = os.RemoveAll(dataDir)
	_ = os.MkdirAll(dataDir, 0755)

	logFilePath := filepath.Join("logs", fmt.Sprintf("%s.log", nodeID))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	args := []string{
		fmt.Sprintf("--node-id=%s", nodeID),
		fmt.Sprintf("--raft-addr=%s", raftAddr),
		fmt.Sprintf("--http-addr=%s", httpAddr),
		fmt.Sprintf("--data-dir=%s", dataDir),
		fmt.Sprintf("--join-addr=%s", joinHTTPAddr),
	}

	cmd := exec.Command(m.executable, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("failed to start process for %s: %w", nodeID, err)
	}

	go func(c *exec.Cmd, f *os.File) {
		_ = c.Wait()
		_ = f.Close()
	}(cmd, logFile)

	procInfo := &ProcessInfo{
		NodeID:    nodeID,
		RaftAddr:  raftAddr,
		HTTPAddr:  httpAddr,
		DataDir:   dataDir,
		PID:       cmd.Process.Pid,
		StartTime: time.Now(),
		cmd:       cmd,
	}

	m.children[nodeID] = procInfo
	log.Printf("[ProcessManager] Spawned node %s (PID %d) -> Raft: %s, HTTP: %s", nodeID, cmd.Process.Pid, raftAddr, httpAddr)

	return procInfo, nil
}

// StopProcess terminates a process (checks both local map and system-wide by exact node ID)
func (m *Manager) StopProcess(nodeID string) error {
	m.mu.Lock()
	proc, ok := m.children[nodeID]
	if ok && proc.cmd != nil && proc.cmd.Process != nil {
		log.Printf("[ProcessManager] Stopping managed node process %s (PID %d)...", nodeID, proc.PID)
		_ = proc.cmd.Process.Signal(syscall.SIGKILL)
		delete(m.children, nodeID)
	}
	m.mu.Unlock()

	// Exact pattern match with boundary check to prevent accidental collateral kills (e.g. node-1 vs node-10..node-19)
	pattern := fmt.Sprintf(`node-id=%s(\s|$)`, nodeID)
	_ = exec.Command("pkill", "-9", "-f", "--", pattern).Run()

	return nil
}

// StopAll stops all managed child processes
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for nodeID, proc := range m.children {
		if proc.cmd != nil && proc.cmd.Process != nil {
			log.Printf("[ProcessManager] Terminating child %s (PID %d)", nodeID, proc.PID)
			_ = proc.cmd.Process.Kill()
		}
	}
	m.children = make(map[string]*ProcessInfo)
}

var joinClient = &http.Client{Timeout: 5 * time.Second}

// RequestJoin sends a join request to a remote node's HTTP API
func RequestJoin(targetHTTPAddr, nodeID, raftAddr, httpAddr string) error {
	payload, err := json.Marshal(map[string]string{
		"node_id":   nodeID,
		"raft_addr": raftAddr,
		"http_addr": httpAddr,
	})
	if err != nil {
		return err
	}

	resp, err := joinClient.Post(
		fmt.Sprintf("http://%s/join", targetHTTPAddr),
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join request returned status: %d", resp.StatusCode)
	}

	return nil
}
