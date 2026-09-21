package server

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestCollectJoinKeysIncludesPeerOnlyTombstone(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"

	secondAddress := startMembershipTestRPC(t, second)

	current := ring.New(32)
	current.AddNode(ring.Node{
		ID: "node-1", Address: "127.0.0.1:50051",
	})
	current.AddNode(ring.Node{
		ID: "node-2", Address: secondAddress,
	})
	first.ring, second.ring = current, current
	first.replicationFactor, second.replicationFactor = 2, 2

	if err := first.store.ApplyRecord(10, store.Record{
		Value: "first-only",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := second.store.ApplyRecord(20, store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := first.collectJoinKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{10, 20}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v; want %v", got, want)
	}
}
