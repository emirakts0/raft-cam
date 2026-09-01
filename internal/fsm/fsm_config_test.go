package fsm

import (
	"testing"

	"github.com/hashicorp/raft"
)

func TestApplyConfigurationEntry(t *testing.T) {
	f := NewKVStoreFSM()

	config := raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-1", Address: "127.0.0.1:7001", Suffrage: raft.Voter},
			{ID: "node-4", Address: "127.0.0.1:7004", Suffrage: raft.Voter},
		},
	}

	done := make(chan ConfigEntry, 1)
	f.RegisterApplyConfigCallback(func(e ConfigEntry) { done <- e })

	// hashicorp/raft routes config entries to StoreConfiguration (ConfigurationStore)
	f.StoreConfiguration(7, config)

	entry := <-done
	if entry.Index != 7 {
		t.Errorf("unexpected index: %d", entry.Index)
	}
	if len(entry.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(entry.Servers))
	}
	if entry.Servers[1].ID != "node-4" || !entry.Servers[1].Voter {
		t.Errorf("unexpected server entry: %+v", entry.Servers[1])
	}
}

func TestApplyCommandEntryUnaffected(t *testing.T) {
	f := NewKVStoreFSM()

	var got AppliedEntry
	done := make(chan AppliedEntry, 1)
	f.RegisterApplyCallback(func(e AppliedEntry) { done <- e })

	if ret := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Term: 1, Data: []byte(`{"op":"SET","key":"k","value":"v"}`)}); ret != nil {
		t.Fatalf("unexpected error: %v", ret)
	}

	got = <-done
	if got.Command.Key != "k" {
		t.Errorf("expected key k, got %s", got.Command.Key)
	}
}
