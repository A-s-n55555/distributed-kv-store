package antientropy

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestConfigurationDigestIgnoresNodeOrder(t *testing.T) {
	first := []ring.Node{
		{
			ID:      "node-1",
			Address: "localhost:50051",
		},
		{
			ID:      "node-2",
			Address: "localhost:50052",
		},
	}

	second := []ring.Node{
		first[1],
		first[0],
	}

	a, err := ConfigurationDigest(32, 2, first)
	if err != nil {
		t.Fatal(err)
	}

	b, err := ConfigurationDigest(32, 2, second)
	if err != nil {
		t.Fatal(err)
	}

	if a != b {
		t.Fatal("node insertion order changed configuration digest")
	}
}

func TestConfigurationDigestDetectsChanges(t *testing.T) {
	nodes := []ring.Node{
		{
			ID:      "node-1",
			Address: "localhost:50051",
		},
		{
			ID:      "node-2",
			Address: "localhost:50052",
		},
	}

	original, err := ConfigurationDigest(32, 2, nodes)
	if err != nil {
		t.Fatal(err)
	}

	changedVirtualNodes, _ := ConfigurationDigest(64, 2, nodes)
	if original == changedVirtualNodes {
		t.Fatal("virtual-node change was not detected")
	}

	changedReplication, _ := ConfigurationDigest(32, 1, nodes)
	if original == changedReplication {
		t.Fatal("replication-factor change was not detected")
	}

	changedNodes := append([]ring.Node(nil), nodes...)
	changedNodes[1].Address = "localhost:60052"

	changedMembership, _ := ConfigurationDigest(
		32,
		2,
		changedNodes,
	)
	if original == changedMembership {
		t.Fatal("membership change was not detected")
	}
}

func TestConfigurationDigestRejectsInvalidConfiguration(t *testing.T) {
	validNodes := []ring.Node{
		{
			ID:      "node-1",
			Address: "localhost:50051",
		},
	}

	tests := []struct {
		name              string
		virtualNodeCount  int
		replicationFactor int
		nodes             []ring.Node
	}{
		{
			name:              "zero virtual nodes",
			replicationFactor: 1,
			nodes:             validNodes,
		},
		{
			name:             "zero replication factor",
			virtualNodeCount: 16,
			nodes:            validNodes,
		},
		{
			name:              "insufficient nodes",
			virtualNodeCount:  16,
			replicationFactor: 2,
			nodes:             validNodes,
		},
		{
			name:              "duplicate node",
			virtualNodeCount:  16,
			replicationFactor: 1,
			nodes: []ring.Node{
				{
					ID:      "node-1",
					Address: "localhost:50051",
				},
				{
					ID:      "node-1",
					Address: "localhost:50052",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ConfigurationDigest(
				tc.virtualNodeCount,
				tc.replicationFactor,
				tc.nodes,
			)

			if err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}
