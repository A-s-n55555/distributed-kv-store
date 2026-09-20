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
func TestGetReplicasFromEmptyRing(t *testing.T) {
	ring := New(3)

	replicas := ring.GetReplicas("user:1", 2)

	if len(replicas) != 0 {
		t.Fatalf(
			"GetReplicas() returned %d nodes; want 0",
			len(replicas),
		)
	}
}

func TestGetReplicasReturnsDistinctPhysicalNodes(
	t *testing.T,
) {
	ring := New(10)

	ring.AddNode(Node{
		ID:      "node-1",
		Address: "localhost:50051",
	})

	ring.AddNode(Node{
		ID:      "node-2",
		Address: "localhost:50052",
	})

	replicas := ring.GetReplicas("user:42", 2)

	if len(replicas) != 2 {
		t.Fatalf(
			"GetReplicas() returned %d nodes; want 2",
			len(replicas),
		)
	}

	if replicas[0].ID == replicas[1].ID {
		t.Fatalf(
			"GetReplicas() returned duplicate physical node %q",
			replicas[0].ID,
		)
	}

	primary, found := ring.GetNode("user:42")
	if !found {
		t.Fatal("GetNode() did not find the primary node")
	}

	if replicas[0].ID != primary.ID {
		t.Fatalf(
			"first replica = %q; want primary node %q",
			replicas[0].ID,
			primary.ID,
		)
	}
}

func TestGetReplicasLimitsFactorToPhysicalNodeCount(
	t *testing.T,
) {
	ring := New(10)

	ring.AddNode(Node{
		ID:      "node-1",
		Address: "localhost:50051",
	})

	ring.AddNode(Node{
		ID:      "node-2",
		Address: "localhost:50052",
	})

	// Requests five replicas, but only two physical nodes exist.
	replicas := ring.GetReplicas("user:42", 5)

	if len(replicas) != 2 {
		t.Fatalf(
			"GetReplicas() returned %d nodes; want 2",
			len(replicas),
		)
	}
}
func TestNodeCountReturnsPhysicalNodeCount(t *testing.T) {
	ring := New(10)

	node1 := Node{
		ID:      "node-1",
		Address: "localhost:50051",
	}

	node2 := Node{
		ID:      "node-2",
		Address: "localhost:50052",
	}

	ring.AddNode(node1)
	ring.AddNode(node2)

	// Adding the same node again must not increase the count.
	ring.AddNode(node1)

	if ring.NodeCount() != 2 {
		t.Fatalf(
			"NodeCount() = %d; want 2",
			ring.NodeCount(),
		)
	}
}

func TestConfigurationIsSortedAndIndependent(t *testing.T) {
	r := New(32)

	r.AddNode(Node{
		ID:      "node-2",
		Address: "localhost:50052",
	})
	r.AddNode(Node{
		ID:      "node-1",
		Address: "localhost:50051",
	})

	virtualNodes, nodes := r.Configuration()

	if virtualNodes != 32 {
		t.Fatalf(
			"virtual-node count = %d; want 32",
			virtualNodes,
		)
	}

	if len(nodes) != 2 ||
		nodes[0].ID != "node-1" ||
		nodes[1].ID != "node-2" {
		t.Fatalf("incorrect node order: %+v", nodes)
	}

	nodes[0].ID = "changed"
	nodes = append(nodes, Node{ID: "injected"})

	_, unchanged := r.Configuration()

	if len(unchanged) != 2 ||
		unchanged[0].ID != "node-1" {
		t.Fatalf(
			"returned configuration exposed ring state: %+v",
			unchanged,
		)
	}
}
