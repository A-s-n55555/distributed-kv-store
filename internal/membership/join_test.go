package membership

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestPrepareJoinDoesNotChangeCurrentConfiguration(t *testing.T) {
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
	originalIdentity := current.Identity()

	proposed, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	if current.Identity() != originalIdentity ||
		len(current.Nodes()) != 2 ||
		oldRing.NodeCount() != 2 {
		t.Fatal("preparing a join changed current membership")
	}
	if proposed.Identity().Epoch != 2 ||
		proposed.Identity().Digest == current.Identity().Digest ||
		len(proposed.Nodes()) != 3 ||
		proposed.Ring().NodeCount() != 3 {
		t.Fatal("incorrect proposed membership")
	}

	// Editing a ring returned by the configuration must not edit it.
	copyOfProposed := proposed.Ring()
	copyOfProposed.AddNode(ring.Node{
		ID: "node-4", Address: "localhost:50054",
	})
	if proposed.Ring().NodeCount() != 3 {
		t.Fatal("Ring exposed configuration state")
	}

	foundMovedKey := false
	for key := int64(0); key < 1000; key++ {
		diff, err := ring.DiffReplicasForKey(
			current.Ring(), proposed.Ring(), key, 2,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) == 1 && diff.Added[0].ID == "node-3" {
			foundMovedKey = true
			break
		}
	}
	if !foundMovedKey {
		t.Fatal("joining node received no sampled keys")
	}
}

func TestPrepareJoinRejectsDuplicateIdentity(t *testing.T) {
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

	for _, joining := range []ring.Node{
		{ID: "node-1", Address: "localhost:50053"},
		{ID: "node-3", Address: "localhost:50052"},
	} {
		if _, err := PrepareJoin(current, joining); err == nil {
			t.Fatalf("accepted conflicting node %+v", joining)
		}
	}
}
