package server

import (
	"context"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResolveConcurrentSiblings(t *testing.T) {
	s := newHintDeliveryTestServer(t)

	s.nodeID = "node-1"
	s.ring = ring.New(16)
	s.ring.AddNode(ring.Node{
		ID:      "node-1",
		Address: "localhost:50051",
	})

	s.replicationFactor = 1
	s.readQuorum = 1
	s.writeQuorum = 1

	ctx := context.Background()
	const key int64 = 9400

	for _, record := range []store.Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "second",
			Clock: version.Clock{"node-2": 1},
		},
	} {
		if err := s.store.ApplyRecord(key, record); err != nil {
			t.Fatal(err)
		}
	}

	before, err := s.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}

	if !before.GetConflict() ||
		!before.GetFound() ||
		len(before.GetVersions()) != 2 ||
		before.GetValue() != "" {
		t.Fatalf("expected visible siblings: %+v", before)
	}

	oldContext := version.Clone(
		version.Clock(before.GetContext()),
	)
	if oldContext["node-1"] != 1 ||
		oldContext["node-2"] != 1 {
		t.Fatalf("incorrect sibling context: %v", oldContext)
	}

	// Ordinary Put must not silently choose or merge siblings.
	_, err = s.Put(ctx, &kvpb.PutRequest{
		Key:   key,
		Value: "ordinary-write",
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("Put error = %v; want Aborted", err)
	}

	if len(s.store.GetRecords(key)) != 2 {
		t.Fatal("rejected Put changed the sibling set")
	}

	_, err = s.Resolve(ctx, &kvpb.ResolveRequest{
		Key:     key,
		Value:   "chosen",
		Context: oldContext,
	})
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	after, err := s.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}

	if after.GetConflict() ||
		!after.GetFound() ||
		after.GetValue() != "chosen" ||
		len(after.GetVersions()) != 1 {
		t.Fatalf("incorrect resolved response: %+v", after)
	}

	resolvedClock := version.Clock(
		after.GetVersions()[0].GetClock(),
	)

	for _, previous := range before.GetVersions() {
		if version.Compare(
			resolvedClock,
			version.Clock(previous.GetClock()),
		) != version.After {
			t.Fatalf(
				"resolved clock %v does not supersede %v",
				resolvedClock,
				previous.GetClock(),
			)
		}
	}

	// The original sibling context is now stale.
	_, err = s.Resolve(ctx, &kvpb.ResolveRequest{
		Key:     key,
		Value:   "stale-attempt",
		Context: oldContext,
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale Resolve error = %v; want Aborted", err)
	}

	final, err := s.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}

	if final.GetValue() != "chosen" ||
		final.GetConflict() ||
		len(final.GetVersions()) != 1 ||
		version.Compare(
			version.Clock(final.GetVersions()[0].GetClock()),
			resolvedClock,
		) != version.Equal {
		t.Fatalf("stale resolution changed stored data: %+v", final)
	}
}
