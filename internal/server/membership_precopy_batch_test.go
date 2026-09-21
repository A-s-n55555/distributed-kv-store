package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestPreCopyJoinInventoryIncludesPeerOnlyKey(t *testing.T) {
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
	for _, service := range []*GRPCServer{first, second} {
		service.ring = current
		service.replicationFactor = 2
	}

	var moved []int64
	stableKey := int64(-1)
	for key := int64(0); key < 10000; key++ {
		diff, err := ring.DiffReplicasForKey(
			current, proposed, key, 2,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) > 0 && len(moved) < 2 {
			moved = append(moved, key)
		}
		if len(diff.Added) == 0 && stableKey == -1 {
			stableKey = key
		}
		if len(moved) == 2 && stableKey != -1 {
			break
		}
	}
	if len(moved) != 2 || stableKey == -1 {
		t.Fatal("could not find moved and unchanged keys")
	}

	peerOnly := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}
	live := store.Record{
		Value: "live",
		Clock: version.Clock{"node-1": 1},
	}
	tombstone := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 2},
	}
	stable := store.Record{
		Value: "unchanged",
		Clock: version.Clock{"node-1": 2},
	}

	for _, item := range []struct {
		service *GRPCServer
		key     int64
		record  store.Record
	}{
		{second, moved[0], peerOnly},
		{first, moved[1], live},
		{second, moved[1], tombstone},
		{first, stableKey, stable},
	} {
		if err := item.service.store.ApplyRecord(
			item.key, item.record,
		); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	progress, err := first.preCopyJoinInventory(ctx, proposed)
	if err != nil {
		t.Fatal(err)
	}
	if progress.PlannedKeys != 2 ||
		progress.CompletedKeys != 2 ||
		progress.RecordsSent != 3 {
		t.Fatalf("unexpected progress: %+v", progress)
	}

	if got := target.store.GetRecords(moved[0]); len(got) != 1 ||
		!containsRecord(got, peerOnly) {
		t.Fatalf("missing peer-only tombstone: %+v", got)
	}
	if got := target.store.GetRecords(moved[1]); len(got) != 2 ||
		!containsRecord(got, live) ||
		!containsRecord(got, tombstone) {
		t.Fatalf("missing sibling set: %+v", got)
	}
	if got := target.store.GetRecords(stableKey); len(got) != 0 {
		t.Fatalf("unchanged key was unnecessarily copied: %+v", got)
	}

	// A newer tombstone arrives after the initial pre-copy.
	late := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 3, "node-2": 2},
	}
	if err := first.store.ApplyRecord(moved[1], late); err != nil {
		t.Fatal(err)
	}

	active, err := membership.NewConfiguration(1, current, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: targetAddress,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, service := range []*GRPCServer{first, second} {
		if err := service.SetActiveMembership(active); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureJoinPause(
			filepath.Join(t.TempDir(), service.nodeID+"-pause.json"),
		); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureMembershipControlToken(
			strings.Repeat("ab", 32),
		); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := first.CatchUpJoin(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	got := target.store.GetRecords(moved[1])
	if len(got) != 1 || !containsRecord(got, late) {
		t.Fatalf("new owner missed the post-copy tombstone: %+v", got)
	}
	if !first.writesPaused || !second.writesPaused {
		t.Fatal("old members must remain paused after catch-up")
	}
}
