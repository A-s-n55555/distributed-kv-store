package membership

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestJoinPauseMarkerSurvivesReloadAndRejectsAnotherCandidate(t *testing.T) {
	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	oldRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

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

	path := filepath.Join(t.TempDir(), "join-pause.json")
	if _, err := LoadJoinPause(path, current); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing marker: got %v, want file-not-exist error", err)
	}
	if err := SaveJoinPause(path, current, candidate); err != nil {
		t.Fatal(err)
	}
	if err := SaveJoinPause(path, current, candidate); err != nil {
		t.Fatalf("repeat same candidate: %v", err)
	}

	other, err := PrepareJoin(current, ring.Node{
		ID: "node-4", Address: "localhost:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveJoinPause(path, current, other); err == nil {
		t.Fatal("replaced an existing pause with another candidate")
	}

	reloaded, err := LoadJoinPause(path, current)
	if err != nil || reloaded.Identity() != candidate.Identity() {
		t.Fatalf("reloaded identity = %v, error = %v", reloaded.Identity(), err)
	}
}

func TestJoinPauseDoesNotOverwriteCorruptMarker(t *testing.T) {
	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	oldRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

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

	path := filepath.Join(t.TempDir(), "join-pause.json")
	if err := os.WriteFile(path, []byte("invalid marker"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SaveJoinPause(path, current, candidate); err == nil {
		t.Fatal("overwrote a corrupt pause marker")
	}
}
