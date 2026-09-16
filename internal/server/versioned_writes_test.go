package server

import (
	"context"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

func TestVersionedPutDeleteAndRecreate(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"
	s.replicationFactor = 1
	s.readQuorum = 1
	s.writeQuorum = 1

	s.ring = ring.New(10)
	s.ring.AddNode(ring.Node{ID: "node-1"})

	ctx := context.Background()

	if _, err := s.Put(ctx, &kvpb.PutRequest{
		Key:   1,
		Value: "first",
	}); err != nil {
		t.Fatal(err)
	}

	first, _ := s.store.GetRecord(1)
	if first.Clock["node-1"] != 1 {
		t.Fatalf("first clock = %v; want node-1:1", first.Clock)
	}

	if _, err := s.Put(ctx, &kvpb.PutRequest{
		Key:   1,
		Value: "second",
	}); err != nil {
		t.Fatal(err)
	}

	second, _ := s.store.GetRecord(1)
	if second.Value != "second" ||
		version.Compare(second.Clock, first.Clock) != version.After {
		t.Fatalf("incorrect second record: %+v", second)
	}

	if _, err := s.Delete(
		ctx,
		&kvpb.DeleteRequest{Key: 1},
	); err != nil {
		t.Fatal(err)
	}

	tombstone, _ := s.store.GetRecord(1)
	if !tombstone.Deleted ||
		version.Compare(tombstone.Clock, second.Clock) != version.After {
		t.Fatalf("incorrect tombstone: %+v", tombstone)
	}

	response, err := s.Get(ctx, &kvpb.GetRequest{Key: 1})
	if err != nil {
		t.Fatal(err)
	}

	if response.GetFound() {
		t.Fatal("deleted key was returned as live")
	}

	if _, err := s.Put(ctx, &kvpb.PutRequest{
		Key:   1,
		Value: "recreated",
	}); err != nil {
		t.Fatal(err)
	}

	recreated, _ := s.store.GetRecord(1)
	if recreated.Deleted ||
		recreated.Value != "recreated" ||
		version.Compare(recreated.Clock, tombstone.Clock) != version.After {
		t.Fatalf("incorrect recreated record: %+v", recreated)
	}
}
