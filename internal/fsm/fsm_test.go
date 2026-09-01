package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type mockSnapshotSink struct {
	buf    *bytes.Buffer
	closed bool
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot-1"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func TestKVStoreFSM_ApplyAndGet(t *testing.T) {
	store := NewKVStoreFSM()

	appliedCh := make(chan AppliedEntry, 10)
	store.RegisterApplyCallback(func(entry AppliedEntry) {
		appliedCh <- entry
	})

	cmd := Command{
		Op:        OpSet,
		Key:       "foo",
		Value:     "bar",
		Timestamp: time.Now(),
	}
	cmdBytes, _ := json.Marshal(cmd)

	logEntry := &raft.Log{
		Index: 1,
		Term:  1,
		Data:  cmdBytes,
	}

	res := store.Apply(logEntry)
	if res != nil {
		t.Fatalf("unexpected error from Apply: %v", res)
	}

	val, ok := store.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("expected 'bar', got '%s', ok=%v", val, ok)
	}

	select {
	case entry := <-appliedCh:
		if entry.Command.Key != "foo" || entry.Command.Value != "bar" {
			t.Errorf("unexpected callback entry: %+v", entry)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("expected callback to be called")
	}

	// Test Delete
	delCmd := Command{
		Op:        OpDelete,
		Key:       "foo",
		Timestamp: time.Now(),
	}
	delBytes, _ := json.Marshal(delCmd)
	store.Apply(&raft.Log{Index: 2, Term: 1, Data: delBytes})

	_, ok = store.Get("foo")
	if ok {
		t.Fatalf("expected 'foo' to be deleted")
	}

	select {
	case entry := <-appliedCh:
		if entry.Command.Op != OpDelete || entry.Command.Key != "foo" {
			t.Errorf("unexpected delete callback entry: %+v", entry)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("expected delete callback to be called")
	}
}

func TestKVStoreFSM_SnapshotAndRestore(t *testing.T) {
	store := NewKVStoreFSM()
	cmd := Command{Op: OpSet, Key: "key1", Value: "val1"}
	data, _ := json.Marshal(cmd)
	store.Apply(&raft.Log{Index: 1, Term: 1, Data: data})

	snap, err := store.Snapshot()
	if err != nil {
		t.Fatalf("failed to take snapshot: %v", err)
	}

	sink := &mockSnapshotSink{buf: new(bytes.Buffer)}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot: %v", err)
	}

	// Restore in a fresh FSM
	newStore := NewKVStoreFSM()
	rc := io.NopCloser(bytes.NewReader(sink.buf.Bytes()))
	if err := newStore.Restore(rc); err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	val, ok := newStore.Get("key1")
	if !ok || val != "val1" {
		t.Fatalf("expected 'val1' in restored store, got '%s'", val)
	}
}
