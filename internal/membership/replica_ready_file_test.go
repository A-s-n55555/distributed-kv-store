package membership

import (
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestJoinReplicaReadyPersistence(t *testing.T) {
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

	if err := RecordJoinReplicaReady(pausePath, candidate); err == nil {
		t.Fatal("accepted replica readiness before activation")
	}

	if err := ActivateCommittedJoin(
		activePath, pausePath, current, candidate,
	); err != nil {
		t.Fatal(err)
	}

	if ready, err := LoadJoinReplicaReady(
		pausePath, candidate,
	); err != nil || ready {
		t.Fatalf("before readiness: ready=%v, err=%v", ready, err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := RecordJoinReplicaReady(pausePath, candidate); err != nil {
			t.Fatalf("record attempt %d: %v", attempt+1, err)
		}
	}

	if ready, err := LoadJoinReplicaReady(
		pausePath, candidate,
	); err != nil || !ready {
		t.Fatalf("reload readiness: ready=%v, err=%v", ready, err)
	}

	if _, err := LoadJoinPause(pausePath, current); err != nil {
		t.Fatalf("readiness damaged the retained pause: %v", err)
	}

	different, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordJoinReplicaReady(pausePath, different); err == nil {
		t.Fatal("accepted readiness for another candidate")
	}
}
