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

	record, exists := mustStoreGetRecord(t, s.store, 1)
	if !exists ||
		record.Value != "delivered" ||
		record.Clock["node-2"] != 3 {
		t.Fatalf("incorrect delivered record: %+v", record)
	}
}

func TestHintDeliveryAcceptsConcurrentSibling(t *testing.T) {
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

	if delivered != 1 {
		t.Fatalf("delivered hints = %d; want 1", delivered)
	}

	if pending := s.hintQueue.Pending(); len(pending) != 0 {
		t.Fatalf("pending hints = %d; want 0", len(pending))
	}

	records := s.store.GetRecords(1)
	if len(records) != 2 {
		t.Fatalf("stored siblings = %d; want 2", len(records))
	}

	// Check that both values survived; do not assume sibling ordering.
	values := map[string]bool{}
	for _, record := range records {
		values[record.Value] = true
	}

	if !values["existing"] || !values["concurrent"] {
		t.Fatalf("expected both sibling values; got %+v", records)
	}
}
