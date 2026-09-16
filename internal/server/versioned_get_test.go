package server

import (
	"context"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

func TestGetUsesVersionedRecords(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"
	s.replicationFactor = 1
	s.readQuorum = 1

	s.ring = ring.New(10)
	s.ring.AddNode(ring.Node{
		ID:      "node-1",
		Address: "localhost:50051",
	})

	ctx := context.Background()

	if err := s.store.ApplyRecord(1, store.Record{
		Value: "versioned-value",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	response, err := s.Get(ctx, &kvpb.GetRequest{Key: 1})
	if err != nil {
		t.Fatal(err)
	}

	if !response.GetFound() ||
		response.GetValue() != "versioned-value" {
		t.Fatalf("incorrect value response: %v", response)
	}

	if err := s.store.ApplyRecord(1, store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 2},
	}); err != nil {
		t.Fatal(err)
	}

	response, err = s.Get(ctx, &kvpb.GetRequest{Key: 1})
	if err != nil {
		t.Fatal(err)
	}

	if response.GetFound() || response.GetValue() != "" {
		t.Fatalf("Get() exposed a tombstone: %v", response)
	}

	response, err = s.Get(ctx, &kvpb.GetRequest{Key: 99})
	if err != nil {
		t.Fatal(err)
	}

	if response.GetFound() {
		t.Fatal("Get() reported a missing key as found")
	}
}
