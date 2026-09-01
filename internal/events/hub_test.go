package events

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestHub_LazyActivationAndBroadcast(t *testing.T) {
	var activatedCount int32
	var deactivatedCount int32

	hub := NewHub(
		func() { atomic.AddInt32(&activatedCount, 1) },
		func() { atomic.AddInt32(&deactivatedCount, 1) },
	)

	if hub.IsActive() {
		t.Fatalf("expected hub to be inactive initially")
	}

	// Broadcast while inactive should be a no-op
	hub.Broadcast(Event{Type: EventTermChanged, Message: "test"})

	// Subscribe first client -> should trigger activation
	ch1, unsub1 := hub.Subscribe()
	if !hub.IsActive() {
		t.Fatalf("expected hub to be active after first subscriber")
	}

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&activatedCount) != 1 {
		t.Fatalf("expected onActivate to be called once, got %d", activatedCount)
	}

	// Subscribe second client -> should NOT trigger another activation
	ch2, unsub2 := hub.Subscribe()
	if hub.SubscriberCount() != 2 {
		t.Fatalf("expected 2 subscribers, got %d", hub.SubscriberCount())
	}

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&activatedCount) != 1 {
		t.Fatalf("expected onActivate to still be 1, got %d", activatedCount)
	}

	// Broadcast event -> both clients should receive
	hub.Broadcast(Event{Type: EventLeaderChanged, Message: "leader 1"})

	select {
	case evt := <-ch1:
		if evt.Type != EventLeaderChanged {
			t.Errorf("ch1 received unexpected type: %s", evt.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("ch1 timed out waiting for event")
	}

	select {
	case evt := <-ch2:
		if evt.Type != EventLeaderChanged {
			t.Errorf("ch2 received unexpected type: %s", evt.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("ch2 timed out waiting for event")
	}

	// Unsubscribe first client
	unsub1()
	if !hub.IsActive() {
		t.Fatalf("expected hub to remain active with 1 subscriber")
	}

	// Unsubscribe second client -> should trigger deactivation
	unsub2()
	if hub.IsActive() {
		t.Fatalf("expected hub to be inactive with 0 subscribers")
	}

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&deactivatedCount) != 1 {
		t.Fatalf("expected onDeactivate to be called once, got %d", deactivatedCount)
	}
}

func TestHub_RingBufferCache(t *testing.T) {
	hub := NewHub(nil, nil)

	// Broadcast 60 events while inactive (e.g. background Raft transitions)
	for i := 1; i <= 60; i++ {
		hub.Broadcast(Event{
			Type:    EventTermChanged,
			Message: "event",
			Term:    uint64(i),
		})
	}

	history := hub.GetHistory()
	if len(history) != 50 {
		t.Fatalf("expected exactly 50 cached events, got %d", len(history))
	}

	// Oldest cached event should be event #11 (Term 11)
	if history[0].Term != 11 {
		t.Errorf("expected oldest cached event to have term 11, got %d", history[0].Term)
	}

	// Newest cached event should be event #60 (Term 60)
	if history[49].Term != 60 {
		t.Errorf("expected newest cached event to have term 60, got %d", history[49].Term)
	}
}

func TestHub_DualCacheSeparation(t *testing.T) {
	hub := NewHub(nil, nil)

	// Broadcast 10 Raft events and 30 real heartbeat RPC events
	for i := 1; i <= 10; i++ {
		hub.Broadcast(Event{
			Type:    EventLogApplied,
			Message: "apply",
			Term:    uint64(i),
		})
	}
	for i := 1; i <= 30; i++ {
		hub.Broadcast(Event{
			Type:    EventHeartbeatSent,
			Message: "hb",
			Term:    uint64(i),
			Data: map[string]any{
				"peer_id":      "node-2",
				"term":         uint64(i),
				"commit_index": uint64(i),
			},
		})
	}

	// Heartbeat RPCs must not appear in the raft lifecycle cache
	raftEvents := hub.GetHistory()
	if len(raftEvents) != 10 {
		t.Fatalf("expected 10 raft events preserved in raft cache, got %d", len(raftEvents))
	}

	// Coalesced: the 30 individual heartbeats accumulate, flush produces one batch
	hub.hbFlushOnce()

	hbEvents := hub.GetHeartbeatHistory()
	if len(hbEvents) != 1 {
		t.Fatalf("expected 1 coalesced heartbeat batch, got %d", len(hbEvents))
	}
	batch := hbEvents[0]
	if batch.Type != EventHeartbeatSent {
		t.Fatalf("expected batch type HEARTBEAT_SENT, got %s", batch.Type)
	}
	data := batch.Data.(map[string]interface{})
	if data["count"] != 30 {
		t.Errorf("expected batch count 30, got %v", data["count"])
	}
	if batch.Term != 30 {
		t.Errorf("expected latest heartbeat batch term 30, got %d", batch.Term)
	}
}

func TestHub_HeartbeatHistoryCap(t *testing.T) {
	hub := NewHub(nil, nil)

	// Flush 20 heartbeat batches: only the last 10 are retained
	for i := 1; i <= 20; i++ {
		hub.Broadcast(Event{
			Type: EventHeartbeatSent,
			Data: map[string]any{"peer_id": "node-2", "term": uint64(i)},
		})
		hub.hbFlushOnce()
	}

	hbEvents := hub.GetHeartbeatHistory()
	if len(hbEvents) != 10 {
		t.Fatalf("expected heartbeat cache capped at 10, got %d", len(hbEvents))
	}
	if hbEvents[0].Term != 11 {
		t.Errorf("expected oldest retained heartbeat batch term 11, got %d", hbEvents[0].Term)
	}
	if hbEvents[9].Term != 20 {
		t.Errorf("expected newest retained heartbeat batch term 20, got %d", hbEvents[9].Term)
	}
}

func TestHub_HeartbeatCoalescingByDirection(t *testing.T) {
	hub := NewHub(nil, nil)

	// Leader-role node sends heartbeats to two peers; follower-role node receives some
	for i := 0; i < 5; i++ {
		hub.Broadcast(Event{
			Type: EventHeartbeatSent,
			Data: map[string]any{"peer_id": "node-2", "term": uint64(7)},
		})
		hub.Broadcast(Event{
			Type: EventHeartbeatSent,
			Data: map[string]any{"peer_id": "node-3", "term": uint64(7)},
		})
		hub.Broadcast(Event{
			Type: EventHeartbeatReceived,
			Data: map[string]any{"peer_id": "node-1", "term": uint64(7)},
		})
	}

	hub.hbFlushOnce()

	hbEvents := hub.GetHeartbeatHistory()
	if len(hbEvents) != 2 {
		t.Fatalf("expected 2 coalesced batches (sent + received), got %d", len(hbEvents))
	}

	for _, evt := range hbEvents {
		data := evt.Data.(map[string]interface{})
		switch evt.Type {
		case EventHeartbeatSent:
			if data["count"] != 10 {
				t.Errorf("expected sent batch count 10, got %v", data["count"])
			}
			targets := data["targets"].([]string)
			if len(targets) != 2 || targets[0] != "node-2" || targets[1] != "node-3" {
				t.Errorf("expected sent batch targets [node-2 node-3], got %v", targets)
			}
		case EventHeartbeatReceived:
			if data["count"] != 5 {
				t.Errorf("expected received batch count 5, got %v", data["count"])
			}
		default:
			t.Errorf("unexpected batch type: %s", evt.Type)
		}
	}
}
