package membership

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestStagingPromotionPersistence(t *testing.T) {
	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{
		ID: "node-1", Address: "localhost:50051",
	})
	oldRing.AddNode(ring.Node{
		ID: "node-2", Address: "localhost:50052",
	})

	current, err := NewConfiguration(1, oldRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	pausePath := filepath.Join(t.TempDir(), "membership-pause.json")

	// A previous snapshot alone must not count as a promotion decision.
	if err := SaveCandidate(previousJoinPath(pausePath), current); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadStagingPromotion(
		pausePath, "node-3",
	); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("without decision marker: got %v, want ErrNotExist", err)
	}

	// An old member cannot use the joining-node promotion path.
	if err := RecordStagingPromotion(
		pausePath, "node-1", current, candidate,
	); err == nil {
		t.Fatal("accepted promotion of an old member")
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := RecordStagingPromotion(
			pausePath, "node-3", current, candidate,
		); err != nil {
			t.Fatalf("record promotion attempt %d: %v", attempt+1, err)
		}
	}

	previous, recovered, err := LoadStagingPromotion(pausePath, "node-3")
	if err != nil {
		t.Fatal(err)
	}
	if previous.Identity() != current.Identity() ||
		recovered.Identity() != candidate.Identity() {
		t.Fatal("recovered promotion has different identities")
	}

	different, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordStagingPromotion(
		pausePath, "node-3", current, different,
	); err == nil {
		t.Fatal("overwrote promotion with a different candidate")
	}

	// A decision marker with missing recovery data must be an error,
	// not be mistaken for a node that has never been promoted.
	if err := os.Remove(previousJoinPath(pausePath)); err != nil {
		t.Fatal(err)
	}
	_, _, err = LoadStagingPromotion(pausePath, "node-3")
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing previous membership was not rejected: %v", err)
	}
}
