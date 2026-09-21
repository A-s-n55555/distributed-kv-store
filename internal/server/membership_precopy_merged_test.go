package server

import (
	"context"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestPreCopyFromOldOwnersIncludesPeerOnlyAndSplitVersions(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	target := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"
	target.nodeID = "node-3"

	secondAddress := startMembershipTestRPC(t, second)
	targetAddress := startMembershipTestRPC(t, target)

	current := ring.New(32)
	proposed := ring.New(32)
	for _, node := range []ring.Node{
		{ID: "node-1", Address: "127.0.0.1:50051"},
		{ID: "node-2", Address: secondAddress},
	} {
		current.AddNode(node)
		proposed.AddNode(node)
	}
	proposed.AddNode(ring.Node{
		ID: "node-3", Address: targetAddress,
	})
	first.ring = current
	first.replicationFactor = 2

	var moved []int64
	for key := int64(0); key < 1000 && len(moved) < 2; key++ {
		diff, err := ring.DiffReplicasForKey(
			current, proposed, key, 2,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) == 1 && diff.Added[0].ID == "node-3" {
			moved = append(moved, key)
		}
	}
	if len(moved) != 2 {
		t.Fatal("could not find two keys moving to node-3")
	}

	peerOnly := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}
	if err := second.store.ApplyRecord(moved[0], peerOnly); err != nil {
		t.Fatal(err)
	}

	live := store.Record{
		Value: "live",
		Clock: version.Clock{"node-1": 1},
	}
	otherSibling := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 2},
	}
	if err := first.store.ApplyRecord(moved[1], live); err != nil {
		t.Fatal(err)
	}
	if err := second.store.ApplyRecord(moved[1], otherSibling); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sent, err := first.preCopyKeyFromOldOwners(ctx, proposed, moved[0])
	if err != nil || sent != 1 {
		t.Fatalf("peer-only copy: sent=%d, err=%v", sent, err)
	}
	sent, err = first.preCopyKeyFromOldOwners(ctx, proposed, moved[1])
	if err != nil || sent != 2 {
		t.Fatalf("split-sibling copy: sent=%d, err=%v", sent, err)
	}

	if got := target.store.GetRecords(moved[0]); len(got) != 1 ||
		!containsRecord(got, peerOnly) {
		t.Fatalf("missing peer-only tombstone: %+v", got)
	}
	if got := target.store.GetRecords(moved[1]); len(got) != 2 ||
		!containsRecord(got, live) ||
		!containsRecord(got, otherSibling) {
		t.Fatalf("missing split siblings: %+v", got)
	}
}
