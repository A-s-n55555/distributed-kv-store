package membership

import (
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestJoinClientReleasePersistence(t *testing.T) {
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

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	must(SaveCandidate(activePath, current))
	must(SaveJoinPause(pausePath, current, candidate))
	must(RecordJoinCommit(pausePath, current, candidate))
	must(ActivateCommittedJoin(activePath, pausePath, current, candidate))

	if err := RecordJoinClientsReleased(pausePath, candidate); err == nil {
		t.Fatal("released clients before replica readiness")
	}

	must(RecordJoinReplicaReady(pausePath, candidate))

	if err := RecordJoinClientReleaseDecision(
		pausePath, "node-1", candidate,
	); err == nil {
		t.Fatal("authorized client release without resume decision")
	}

	must(RecordJoinResumeDecision(pausePath, "node-1", candidate))

	if err := RecordJoinClientReleaseDecision(
		pausePath, "node-2", candidate,
	); err == nil {
		t.Fatal("non-coordinator authorized client release")
	}

	for attempt := 0; attempt < 2; attempt++ {
		must(RecordJoinClientReleaseDecision(pausePath, "node-1", candidate))
		must(RecordJoinClientsReleased(pausePath, candidate))
	}

	decided, err := LoadJoinClientReleaseDecision(pausePath, candidate)
	must(err)
	released, err := LoadJoinClientsReleased(pausePath, candidate)
	must(err)
	if !decided || !released {
		t.Fatal("release decision or local completion was lost")
	}

	// Recovery evidence must remain available after release.
	_, err = LoadActivatedJoin(pausePath, candidate)
	must(err)

	different, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50054",
	})
	must(err)
	if _, err := LoadJoinClientsReleased(pausePath, different); err == nil {
		t.Fatal("accepted release for another candidate")
	}
}
