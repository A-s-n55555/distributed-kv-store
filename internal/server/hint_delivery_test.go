package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/handoff"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func newHintDeliveryTestServer(t *testing.T) *GRPCServer {
	t.Helper()

	s := newRecordTestServer(t)
	s.nodeID = "node-1"

	queue, err := handoff.OpenQueue(
		filepath.Join(t.TempDir(), "hints.log"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := queue.Close(); err != nil {
			t.Errorf("queue Close() error = %v", err)
		}
	})

	s.hintQueue = queue
	return s
}

func TestHintDeliveryAppliesAndAcknowledges(t *testing.T) {
	s := newHintDeliveryTestServer(t)

	hint, err := handoff.NewHint(
		ring.Node{
			ID:      "node-1",
			Address: "localhost:50051",
		},
		1,
		store.Record{
			Value: "delivered",
			Clock: version.Clock{"node-2": 3},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.hintQueue.Enqueue(hint); err != nil {
		t.Fatal(err)
	}

	delivered, err := s.deliverPendingHints(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if delivered != 1 || len(s.hintQueue.Pending()) != 0 {
		t.Fatalf(
			"delivered=%d pending=%d; want 1 and 0",
			delivered,
			len(s.hintQueue.Pending()),
		)
	}

	record, exists := s.store.GetRecord(1)
	if !exists ||
		record.Value != "delivered" ||
		record.Clock["node-2"] != 3 {
		t.Fatalf("incorrect delivered record: %+v", record)
	}
}

func TestHintDeliveryRetainsConcurrentConflict(t *testing.T) {
	s := newHintDeliveryTestServer(t)

	if err := s.store.ApplyRecord(1, store.Record{
		Value: "existing",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	hint, err := handoff.NewHint(
		ring.Node{
			ID:      "node-1",
			Address: "localhost:50051",
		},
		1,
		store.Record{
			Value: "concurrent",
			Clock: version.Clock{"node-2": 1},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.hintQueue.Enqueue(hint); err != nil {
		t.Fatal(err)
	}

	delivered, err := s.deliverPendingHints(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if delivered != 0 || len(s.hintQueue.Pending()) != 1 {
		t.Fatal("conflicting hint was incorrectly acknowledged")
	}

	value, _ := s.store.Get(1)
	if value != "existing" {
		t.Fatal("conflicting hint replaced the existing value")
	}
}
