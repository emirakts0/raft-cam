package fsm

import (
	"io"
	"strings"
	"testing"
	"time"
)

// TestRestoreCallbackFires verifies that a successful raft.FSM.Restore invokes
// the registered restore listener with the recovered key count
func TestRestoreCallbackFires(t *testing.T) {
	f := NewKVStoreFSM()

	var gotKeys = -1
	f.RegisterRestoreCallback(func(keys int) { gotKeys = keys })

	snap := `{"user_name":"ahmet","city":"ankara"}`
	if err := f.Restore(io.NopCloser(strings.NewReader(snap))); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Callback runs in its own goroutine; wait for it briefly
	for i := 0; i < 100 && gotKeys < 0; i++ {
		time.Sleep(time.Millisecond)
	}
	if gotKeys != 2 {
		t.Fatalf("restore callback not invoked with key count: got %d, want 2", gotKeys)
	}

	if v, ok := f.Get("user_name"); !ok || v != "ahmet" {
		t.Fatalf("restored data incorrect: %q %v", v, ok)
	}
}

// TestRestoreCallbackNotFiredOnBadData verifies a failed decode never fires
// the callback (no synthetic events from failed snapshot restores)
func TestRestoreCallbackNotFiredOnBadData(t *testing.T) {
	f := NewKVStoreFSM()

	fired := false
	f.RegisterRestoreCallback(func(keys int) { fired = true })

	if err := f.Restore(io.NopCloser(strings.NewReader("not-json"))); err == nil {
		t.Fatal("Restore should fail on invalid JSON")
	}

	time.Sleep(10 * time.Millisecond)
	if fired {
		t.Fatal("restore callback must not fire on failed restore")
	}
}
