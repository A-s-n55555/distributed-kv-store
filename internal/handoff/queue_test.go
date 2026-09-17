package handoff

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestQueueRecoveryAndAcknowledgment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hints.log")

	hint, err := NewHint(
		ring.Node{
			ID:      "node-2",
			Address: "localhost:50052",
		},
		1,
		store.Record{
			Value: "pending",
			Clock: version.Clock{"node-1": 1},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	q1, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := q1.Enqueue(hint); err != nil {
		_ = q1.Close()
		t.Fatal(err)
	}

	// Identical enqueue is idempotent.
	if err := q1.Enqueue(hint); err != nil {
		_ = q1.Close()
		t.Fatal(err)
	}

	// Mutating the caller's record must not affect the queue.
	hint.Record.Clock["node-1"] = 999

	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}

	q2, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}

	pending := q2.Pending()

	if len(pending) != 1 ||
		pending[0].Record.Value != "pending" ||
		pending[0].Record.Clock["node-1"] != 1 {
		_ = q2.Close()
		t.Fatalf("incorrect recovered hints: %+v", pending)
	}

	// Returned snapshots must not expose internal maps.
	pending[0].Record.Clock["node-1"] = 888

	if q2.Pending()[0].Record.Clock["node-1"] != 1 {
		_ = q2.Close()
		t.Fatal("Pending() exposed the internal clock")
	}

	if err := q2.Ack(hint.ID); err != nil {
		_ = q2.Close()
		t.Fatal(err)
	}

	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}

	q3, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()

	if len(q3.Pending()) != 0 {
		t.Fatal("acknowledged hint returned after recovery")
	}
}

func TestQueueRejectsTruncatedJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hints.log")

	if err := os.WriteFile(
		path,
		[]byte(`{"operation":"ENQUEUE","hint":`),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	q, err := OpenQueue(path)
	if err == nil {
		_ = q.Close()
		t.Fatal("OpenQueue() accepted a truncated journal")
	}
}
