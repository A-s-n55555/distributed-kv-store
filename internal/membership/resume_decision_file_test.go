package membership

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestJoinResumeDecisionPersistence(t *testing.T) {
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

	directory := t.TempDir()
	activePath := filepath.Join(directory, "membership-active.json")
	pausePath := filepath.Join(directory, "membership-pause.json")

	if err := SaveCandidate(activePath, current); err != nil {
		t.Fatal(err)
	}
	if err := SaveJoinPause(pausePath, current, candidate); err != nil {
		t.Fatal(err)
	}
	if err := RecordJoinCommit(pausePath, current, candidate); err != nil {
		t.Fatal(err)
	}

	if err := RecordJoinResumeDecision(
		pausePath, "node-1", candidate,
	); err == nil {
		t.Fatal("accepted resume before activation")
	}

	if err := ActivateCommittedJoin(
		activePath, pausePath, current, candidate,
	); err != nil {
		t.Fatal(err)
	}

	if recorded, err := LoadJoinResumeDecision(
		pausePath, candidate,
	); err != nil || recorded {
		t.Fatalf("before decision: recorded=%v, err=%v", recorded, err)
	}

	if err := RecordJoinResumeDecision(
		pausePath, "node-2", candidate,
	); err == nil {
		t.Fatal("non-coordinator recorded resume")
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := RecordJoinResumeDecision(
			pausePath, "node-1", candidate,
		); err != nil {
			t.Fatalf("record attempt %d: %v", attempt+1, err)
		}
	}

	if recorded, err := LoadJoinResumeDecision(
		pausePath, candidate,
	); err != nil || !recorded {
		t.Fatalf("reload decision: recorded=%v, err=%v", recorded, err)
	}

	// Saving the decision must preserve the durable pause.
	if _, err := LoadJoinPause(pausePath, current); err != nil {
		t.Fatalf("resume decision removed or damaged pause: %v", err)
	}

	different, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadJoinResumeDecision(pausePath, different); err == nil {
		t.Fatal("accepted a decision for another candidate")
	}

	if err := os.WriteFile(
		joinResumeDecisionPath(pausePath), []byte("{invalid"), 0600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadJoinResumeDecision(pausePath, candidate); err == nil {
		t.Fatal("accepted a corrupt resume decision")
	}
}
