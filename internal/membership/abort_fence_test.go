package membership

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestJoinAbortFencePersistsPerCandidate(t *testing.T) {
	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	oldRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

	current, err := NewConfiguration(1, oldRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	first, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareJoin(current, ring.Node{
		ID: "node-4", Address: "localhost:50054",
	})
	if err != nil {
		t.Fatal(err)
	}

	pausePath := filepath.Join(t.TempDir(), "membership-pause.json")
	if aborted, err := JoinWasAborted(pausePath, current, first); err != nil || aborted {
		t.Fatalf("new candidate: aborted=%v, error=%v", aborted, err)
	}
	if err := RecordJoinAbort(pausePath, current, first); err != nil {
		t.Fatal(err)
	}
	if err := RecordJoinAbort(pausePath, current, first); err != nil {
		t.Fatalf("repeat abort: %v", err)
	}
	if aborted, err := JoinWasAborted(pausePath, current, first); err != nil || !aborted {
		t.Fatalf("saved candidate: aborted=%v, error=%v", aborted, err)
	}
	if aborted, err := JoinWasAborted(pausePath, current, second); err != nil || aborted {
		t.Fatalf("different candidate: aborted=%v, error=%v", aborted, err)
	}

	secondPath, err := joinAbortPath(pausePath, second.Identity())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RecordJoinAbort(pausePath, current, second); err == nil {
		t.Fatal("overwrote a corrupt abort marker")
	}
}
