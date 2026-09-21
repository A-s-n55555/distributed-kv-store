package membership

import (
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestLoadJoinCandidateChecksCurrentMembership(t *testing.T) {
	oldRing := ring.New(100)
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

	proposal, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "candidate.json")
	if err := SaveCandidate(path, proposal); err != nil {
		t.Fatal(err)
	}
	loaded, joining, err := LoadJoinCandidate(path, current)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Identity() != proposal.Identity() || joining.ID != "node-3" {
		t.Fatalf("incorrect validated proposal or joining node: %+v", joining)
	}

	// The file remains internally valid, but its epoch is stale.
	stale, err := NewConfiguration(1, proposal.Ring(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveCandidate(path, stale); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadJoinCandidate(path, current); err == nil {
		t.Fatal("accepted a proposal that did not advance the epoch")
	}
}

func TestJoinCandidateRejectsChangedExistingAddress(t *testing.T) {
	oldRing := ring.New(100)
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

	changedRing := ring.New(100)
	changedRing.AddNode(ring.Node{
		ID: "node-1", Address: "localhost:60051",
	})
	changedRing.AddNode(ring.Node{
		ID: "node-2", Address: "localhost:50052",
	})
	changedRing.AddNode(ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	candidate, err := NewConfiguration(2, changedRing, 2)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ValidateJoinCandidate(current, candidate); err == nil {
		t.Fatal("accepted an address change as a node join")
	}
}
