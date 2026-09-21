package membership

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestConfigurationIdentity(t *testing.T) {
	node1 := ring.Node{ID: "node-1", Address: "localhost:50051"}
	node2 := ring.Node{ID: "node-2", Address: "localhost:50052"}
	node3 := ring.Node{ID: "node-3", Address: "localhost:50053"}

	firstRing := ring.New(100)
	firstRing.AddNode(node1)
	firstRing.AddNode(node2)

	reorderedRing := ring.New(100)
	reorderedRing.AddNode(node2)
	reorderedRing.AddNode(node1)

	joinedRing := ring.New(100)
	joinedRing.AddNode(node1)
	joinedRing.AddNode(node2)
	joinedRing.AddNode(node3)

	first, err := NewConfiguration(1, firstRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := NewConfiguration(1, reorderedRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	newEpoch, err := NewConfiguration(2, firstRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := NewConfiguration(2, joinedRing, 2)
	if err != nil {
		t.Fatal(err)
	}

	if first.Identity() != reordered.Identity() {
		t.Fatal("node insertion order changed configuration identity")
	}
	if first.Identity() == newEpoch.Identity() {
		t.Fatal("a new epoch reused the old identity")
	}
	if newEpoch.Identity() == joined.Identity() {
		t.Fatal("different membership reused the same identity")
	}

	nodes := first.Nodes()
	nodes[0].Address = "changed"
	if first.Nodes()[0].Address == "changed" {
		t.Fatal("Nodes exposed configuration state")
	}
}

func TestConfigurationRejectsInvalidInput(t *testing.T) {
	r := ring.New(100)
	r.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})

	if _, err := NewConfiguration(0, r, 1); err == nil {
		t.Fatal("expected zero epoch to be rejected")
	}
	if _, err := NewConfiguration(1, r, 2); err == nil {
		t.Fatal("expected insufficient node count to be rejected")
	}
}
