package ring

import "testing"

func TestDiffReplicasForKeyWhenNodeJoins(t *testing.T) {
	oldRing := New(100)
	newRing := New(100)

	nodes := []Node{
		{ID: "node-1", Address: "localhost:50051"},
		{ID: "node-2", Address: "localhost:50052"},
		{ID: "node-3", Address: "localhost:50053"},
	}
	for _, node := range nodes[:2] {
		oldRing.AddNode(node)
		newRing.AddNode(node)
	}
	newRing.AddNode(nodes[2])

	foundMovedKey := false
	for key := int64(0); key < 1000; key++ {
		diff, err := DiffReplicasForKey(oldRing, newRing, key, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Old) != 2 || len(diff.New) != 2 {
			t.Fatalf("key %d: expected two owners in each ring: %+v", key, diff)
		}
		if len(diff.Added) == 0 {
			continue
		}
		if len(diff.Added) != 1 || diff.Added[0].ID != "node-3" ||
			len(diff.Removed) != 1 {
			t.Fatalf("key %d: unexpected ownership change: %+v", key, diff)
		}
		foundMovedKey = true
		break
	}
	if !foundMovedKey {
		t.Fatal("joining node did not acquire any of the sampled keys")
	}
}

func TestDiffReplicasRejectsInsufficientNewRing(t *testing.T) {
	oldRing := New(100)
	newRing := New(100)
	for _, node := range []Node{
		{ID: "node-1", Address: "localhost:50051"},
		{ID: "node-2", Address: "localhost:50052"},
	} {
		oldRing.AddNode(node)
	}
	newRing.AddNode(Node{ID: "node-1", Address: "localhost:50051"})

	if _, err := DiffReplicasForKey(oldRing, newRing, 42, 2); err == nil {
		t.Fatal("expected rejection when the new ring has fewer nodes than N")
	}
}
