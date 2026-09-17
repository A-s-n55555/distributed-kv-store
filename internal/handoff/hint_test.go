package handoff

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestNewHintCopiesRecord(t *testing.T) {
	target := ring.Node{
		ID:      "node-2",
		Address: "localhost:50052",
	}

	record := store.Record{
		Value: "pending",
		Clock: version.Clock{"node-1": 3},
	}

	hint, err := NewHint(target, 1, record)
	if err != nil {
		t.Fatal(err)
	}

	if err := hint.Validate(); err != nil {
		t.Fatal(err)
	}

	if hint.ID == "" ||
		hint.Target != target ||
		hint.Key != 1 ||
		hint.Record.Value != "pending" {
		t.Fatalf("incorrect hint: %+v", hint)
	}

	record.Clock["node-1"] = 999

	if hint.Record.Clock["node-1"] != 3 {
		t.Fatal("hint retained the caller's clock map")
	}

	cloned := hint.Clone()
	cloned.Record.Clock["node-1"] = 888

	if hint.Record.Clock["node-1"] != 3 {
		t.Fatal("Clone() shared the original clock map")
	}
}

func TestNewHintSupportsTombstones(t *testing.T) {
	hint, err := NewHint(
		ring.Node{
			ID:      "node-2",
			Address: "localhost:50052",
		},
		1,
		store.Record{
			Deleted: true,
			Clock:   version.Clock{"node-1": 4},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !hint.Record.Deleted ||
		hint.Record.Clock["node-1"] != 4 {
		t.Fatalf("incorrect tombstone hint: %+v", hint)
	}
}

func TestNewHintRejectsInvalidPayloads(t *testing.T) {
	target := ring.Node{
		ID:      "node-2",
		Address: "localhost:50052",
	}

	tests := []struct {
		name   string
		target ring.Node
		record store.Record
	}{
		{
			name:   "missing target",
			target: ring.Node{},
			record: store.Record{
				Clock: version.Clock{"node-1": 1},
			},
		},
		{
			name:   "missing clock",
			target: target,
			record: store.Record{Value: "legacy"},
		},
		{
			name:   "zero counter",
			target: target,
			record: store.Record{
				Clock: version.Clock{"node-1": 0},
			},
		},
		{
			name:   "tombstone with value",
			target: target,
			record: store.Record{
				Value:   "invalid",
				Deleted: true,
				Clock:   version.Clock{"node-1": 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewHint(test.target, 1, test.record)

			if err == nil {
				t.Fatal("NewHint() accepted an invalid payload")
			}
		})
	}
}
