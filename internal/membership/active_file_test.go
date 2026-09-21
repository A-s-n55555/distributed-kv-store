package membership

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestLoadOrBootstrapActivePreservesEpochAndRejectsChangedRing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.json")

	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	oldRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

	if _, err := LoadOrBootstrapActive(path, oldRing, 2, false); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: got %v, want file-not-exist error", err)
	}

	active, err := LoadOrBootstrapActive(path, oldRing, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	if active.Identity().Epoch != 1 {
		t.Fatalf("initial epoch = %d; want 1", active.Identity().Epoch)
	}

	reloaded, err := LoadOrBootstrapActive(path, oldRing, 2, false)
	if err != nil || reloaded.Identity() != active.Identity() {
		t.Fatalf("reload = %v, %v; want original identity", reloaded.Identity(), err)
	}

	next, err := PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveCandidate(path, next); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadOrBootstrapActive(path, oldRing, 2, false); err == nil {
		t.Fatal("accepted old startup ring for the saved next epoch")
	}
	loadedNext, err := LoadOrBootstrapActive(path, next.Ring(), 2, false)
	if err != nil || loadedNext.Identity() != next.Identity() {
		t.Fatalf("next epoch = %v, %v; want saved identity", loadedNext.Identity(), err)
	}
}
