package membership

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestPlanLocalTransfersIncludesTombstone(t *testing.T) {
	current := ring.New(100)
	proposed := ring.New(100)

	for _, node := range []ring.Node{
		{ID: "node-1", Address: "localhost:50051"},
		{ID: "node-2", Address: "localhost:50052"},
	} {
		current.AddNode(node)
		proposed.AddNode(node)
	}
	proposed.AddNode(ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})

	var movedKey int64
	found := false
	for key := int64(0); key < 1000; key++ {
		diff, err := ring.DiffReplicasForKey(current, proposed, key, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) > 0 {
			movedKey = key
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no moved key found")
	}

	snapshot := map[int64][]store.Record{
		movedKey: {
			{Deleted: true, Clock: version.Clock{"node-1": 1}},
		},
		movedKey + 1: nil,
	}

	transfers, err := PlanLocalTransfers(
		snapshot, current, proposed, "node-1", 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transfers) != 1 ||
		transfers[0].Key != movedKey ||
		len(transfers[0].Destinations) != 1 ||
		transfers[0].Destinations[0].ID != "node-3" {
		t.Fatalf("unexpected transfers: %+v", transfers)
	}
}
