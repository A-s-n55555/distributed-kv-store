package ring

import "testing"

func TestGetNodeFromEmptyRing(t *testing.T) {
	ring := New(3)

	_, found := ring.GetNode("user:1")

	if found {
		t.Fatal("GetNode() found a node in an empty ring")
	}
}

func TestGetNodeIsDeterministic(t *testing.T) {
	ring := New(10)

	ring.AddNode(Node{
		ID:      "node-1",
		Address: "localhost:50051",
	})

	ring.AddNode(Node{
		ID:      "node-2",
		Address: "localhost:50052",
	})

	firstNode, found := ring.GetNode("user:42")
	if !found {
		t.Fatal("GetNode() did not find a node")
	}

	for i := 0; i < 10; i++ {
		node, found := ring.GetNode("user:42")

		if !found {
			t.Fatal("GetNode() did not find a node")
		}

		if node.ID != firstNode.ID {
			t.Fatalf("GetNode() returned %q; want %q",
				node.ID, firstNode.ID)
		}
	}
}

func TestAddNodeCreatesVirtualNodes(t *testing.T) {
	ring := New(5)

	node := Node{
		ID:      "node-1",
		Address: "localhost:50051",
	}

	ring.AddNode(node)

	if len(ring.entries) != 5 {
		t.Fatalf("virtual node count = %d; want 5",
			len(ring.entries))
	}

	ring.AddNode(node)

	if len(ring.entries) != 5 {
		t.Fatalf("duplicate node added virtual nodes")
	}
}
