package antientropy

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestSharedSnapshotIncludesVersionsAndCopiesClocks(t *testing.T) {
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1"})
	clusterRing.AddNode(ring.Node{ID: "node-2"})

	input := map[int64][]store.Record{
		10: {
			{
				Value: "live",
				Clock: version.Clock{"node-1": 1},
			},
			{
				Deleted: true,
				Clock:   version.Clock{"node-2": 1},
			},
		},
	}

	got, err := SharedSnapshot(
		input, clusterRing, 2, "node-1", "node-2",
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || len(got[10]) != 2 ||
		got[10][0].Value != "live" ||
		!got[10][1].Deleted {
		t.Fatalf("incorrect shared snapshot: %+v", got)
	}

	got[10][0].Value = "changed"
	got[10][0].Clock["node-1"] = 999

	if input[10][0].Value != "live" ||
		input[10][0].Clock["node-1"] != 1 {
		t.Fatal("shared snapshot exposed input state")
	}
}

func TestSharedSnapshotExcludesNonOwner(t *testing.T) {
	clusterRing := ring.New(16)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		clusterRing.AddNode(ring.Node{ID: id})
	}

	const key int64 = 10
	owners := clusterRing.GetReplicas("10", 2)
	if len(owners) != 2 {
		t.Fatal("expected two owners")
	}

	var nonOwner string
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if id != owners[0].ID && id != owners[1].ID {
			nonOwner = id
		}
	}

	input := map[int64][]store.Record{
		key: {
			{
				Value: "value",
				Clock: version.Clock{"node-1": 1},
			},
		},
	}

	included, err := SharedSnapshot(
		input, clusterRing, 2, owners[0].ID, owners[1].ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(included[key]) != 1 {
		t.Fatal("shared owners lost their key")
	}

	excluded, err := SharedSnapshot(
		input, clusterRing, 2, owners[0].ID, nonOwner,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatal("included a key not owned by the peer")
	}

	excluded, err = SharedSnapshot(
		input, clusterRing, 2, nonOwner, owners[0].ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatal("included a key not owned by the local node")
	}
}

func TestSharedSnapshotDetectsRemotelyPresentKey(t *testing.T) {
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1"})
	clusterRing.AddNode(ring.Node{ID: "node-2"})

	local, err := SharedSnapshot(
		nil, clusterRing, 2, "node-1", "node-2",
	)
	if err != nil {
		t.Fatal(err)
	}

	remote, err := SharedSnapshot(
		map[int64][]store.Record{
			10: {
				{
					Deleted: true,
					Clock:   version.Clock{"node-2": 1},
				},
			},
		},
		clusterRing, 2, "node-2", "node-1",
	)
	if err != nil {
		t.Fatal(err)
	}

	different := DifferentBuckets(BuildTree(local), BuildTree(remote))
	if len(different) != 1 || different[0] != BucketForKey(10) {
		t.Fatalf("missing remote key not located: %v", different)
	}
}

func TestSharedSnapshotRejectsInsufficientReplicas(t *testing.T) {
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1"})

	_, err := SharedSnapshot(
		nil, clusterRing, 2, "node-1", "node-2",
	)
	if err == nil {
		t.Fatal("accepted an unsatisfiable replication factor")
	}
}
