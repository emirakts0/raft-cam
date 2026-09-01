package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// OpType represents the operation type on the state machine
type OpType string

const (
	OpSet    OpType = "SET"
	OpDelete OpType = "DELETE"
)

// Command is the payload replicated through Raft log
type Command struct {
	Op        OpType    `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	ClientID  string    `json:"client_id,omitempty"`
}

// AppliedEntry contains details of an applied log entry
type AppliedEntry struct {
	Index     uint64    `json:"index"`
	Term      uint64    `json:"term"`
	Command   Command   `json:"command"`
	AppliedAt time.Time `json:"applied_at"`
}

// ApplyCallback is called whenever a new log entry is successfully committed and applied
type ApplyCallback func(entry AppliedEntry)

// ConfigEntry describes a committed membership (configuration) change
type ConfigEntry struct {
	Index   uint64   `json:"index"`
	Servers []Server `json:"servers"`
}

// Server is a simplified raft.Server for JSON transport
type Server struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Voter   bool   `json:"voter"`
}

// ApplyConfigCallback is called when a configuration change entry is applied
type ApplyConfigCallback func(entry ConfigEntry)

// RestoreCallback is called when the FSM is restored from a snapshot
// (raft.FSM.Restore — fires on InstallSnapshot and on startup recovery)
type RestoreCallback func(keys int)

// KVStoreFSM implements raft.FSM interface
type KVStoreFSM struct {
	mu              sync.RWMutex
	data            map[string]string
	history         []AppliedEntry
	callbacks       []ApplyCallback
	configCallbacks []ApplyConfigCallback
	restoreCallback RestoreCallback
}

// NewKVStoreFSM creates a new key-value store FSM
func NewKVStoreFSM() *KVStoreFSM {
	return &KVStoreFSM{
		data:    make(map[string]string),
		history: make([]AppliedEntry, 0, 100),
	}
}

// RegisterApplyCallback registers a listener for applied log entries
func (f *KVStoreFSM) RegisterApplyCallback(cb ApplyCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbacks = append(f.callbacks, cb)
}

// RegisterApplyConfigCallback registers a listener for applied configuration changes
func (f *KVStoreFSM) RegisterApplyConfigCallback(cb ApplyConfigCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configCallbacks = append(f.configCallbacks, cb)
}

// RegisterRestoreCallback registers a listener for snapshot restores
func (f *KVStoreFSM) RegisterRestoreCallback(cb RestoreCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreCallback = cb
}

// StoreConfiguration implements raft.ConfigurationStore: hashicorp/raft
// routes committed LogConfiguration entries here (never to FSM.Apply)
var _ raft.ConfigurationStore = (*KVStoreFSM)(nil)

func (f *KVStoreFSM) StoreConfiguration(index uint64, config raft.Configuration) {
	f.applyConfigurationFromLog(index, config)
}

// Apply applies a Raft log entry to the key-value store.
// LogConfiguration entries never reach Apply (routed to ConfigurationStore).
func (f *KVStoreFSM) Apply(log *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Op {
	case OpSet:
		f.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(f.data, cmd.Key)
	default:
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}

	applied := AppliedEntry{
		Index:     log.Index,
		Term:      log.Term,
		Command:   cmd,
		AppliedAt: time.Now(),
	}

	// Keep last 100 history items for UI display
	if len(f.history) >= 100 {
		f.history = f.history[1:]
	}
	f.history = append(f.history, applied)

	// Trigger callbacks in separate goroutines to prevent blocking FSM
	for _, cb := range f.callbacks {
		go cb(applied)
	}

	return nil
}

// applyConfigurationFromLog decodes a committed membership change and notifies listeners
func (f *KVStoreFSM) applyConfigurationFromLog(index uint64, config raft.Configuration) {
	servers := make([]Server, 0, len(config.Servers))
	for _, s := range config.Servers {
		servers = append(servers, Server{
			ID:      string(s.ID),
			Address: string(s.Address),
			Voter:   s.Suffrage == raft.Voter,
		})
	}

	entry := ConfigEntry{
		Index:   index,
		Servers: servers,
	}

	f.mu.Lock()
	callbacks := make([]ApplyConfigCallback, len(f.configCallbacks))
	copy(callbacks, f.configCallbacks)
	f.mu.Unlock()

	for _, cb := range callbacks {
		go cb(entry)
	}
}

// Get retrieves a key from the FSM
func (f *KVStoreFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	val, ok := f.data[key]
	return val, ok
}

// GetAll returns a copy of all key-values in the FSM
func (f *KVStoreFSM) GetAll() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return maps.Clone(f.data)
}

// GetHistory returns recent applied log entries
func (f *KVStoreFSM) GetHistory() []AppliedEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return slices.Clone(f.history)
}

// Snapshot returns a snapshot of the current state
func (f *KVStoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &KVSnapshot{data: maps.Clone(f.data)}, nil
}

// Restore restores the state machine from a snapshot
func (f *KVStoreFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string]string
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	f.mu.Lock()
	f.data = data
	cb := f.restoreCallback
	f.mu.Unlock()
	if cb != nil {
		go cb(len(data))
	}
	return nil
}

// KVSnapshot implements raft.FSMSnapshot
type KVSnapshot struct {
	data map[string]string
}

// Persist writes snapshot data to the sink
func (s *KVSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}

// Release is a no-op for in-memory snapshot
func (s *KVSnapshot) Release() {}
