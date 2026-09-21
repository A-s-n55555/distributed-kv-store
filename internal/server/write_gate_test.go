package server

import (
	"context"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPauseCoordinatorWrites(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"
	s.ring = ring.New(16)
	s.ring.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	s.replicationFactor = 1
	s.readQuorum = 1
	s.writeQuorum = 1

	ctx := context.Background()
	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: 1, Value: "before"}); err != nil {
		t.Fatalf("initial Put: %v", err)
	}

	s.pauseCoordinatorWrites()

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: 2, Value: "during"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("paused Put: got %v, want Unavailable", err)
	}
	if _, err := s.Delete(ctx, &kvpb.DeleteRequest{Key: 1}); status.Code(err) != codes.Unavailable {
		t.Fatalf("paused Delete: got %v, want Unavailable", err)
	}
	if _, err := s.Get(ctx, &kvpb.GetRequest{Key: 1}); status.Code(err) != codes.Unavailable {
		t.Fatalf("paused Get: got %v, want Unavailable", err)
	}

	s.resumeCoordinatorWrites()

	response, err := s.Get(ctx, &kvpb.GetRequest{Key: 1})
	if err != nil {
		t.Fatalf("Get after resume: %v", err)
	}
	if !response.GetFound() || response.GetValue() != "before" {
		t.Fatalf("unexpected value after resume: %v", response)
	}

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: 2, Value: "after"}); err != nil {
		t.Fatalf("Put after resume: %v", err)
	}
}
func TestPauseDrainsReplicaApplyPaths(t *testing.T) {
	s := newRecordTestServer(t)
	ctx := context.Background()

	replicaRequest := &kvpb.ReplicaRecordRequest{
		Key: 1,
		Record: &kvpb.VersionedRecord{
			Value: "replica",
			Clock: map[string]uint64{"node-1": 1},
		},
	}

	bucketRecord := &kvpb.VersionedRecord{
		Deleted: true,
		Clock:   map[string]uint64{"node-2": 1},
	}
	bucket := antientropy.BucketForKey(2)
	bucketResponse := &kvpb.AntiEntropyBucketResponse{
		Keys: []*kvpb.AntiEntropyKeyState{
			{Key: 2, Records: []*kvpb.VersionedRecord{bucketRecord}},
		},
	}

	s.pauseCoordinatorWrites()

	if _, err := s.ApplyReplicaRecord(ctx, replicaRequest); status.Code(err) != codes.Unavailable {
		t.Fatalf("paused replica apply: got %v, want Unavailable", err)
	}
	if _, err := s.applyAntiEntropyBucket(bucketResponse, bucket); status.Code(err) != codes.Unavailable {
		t.Fatalf("paused anti-entropy apply: got %v, want Unavailable", err)
	}
	if got := len(s.store.GetRecords(1)); got != 0 {
		t.Fatalf("replica record was stored during pause: %d versions", got)
	}
	if got := len(s.store.GetRecords(2)); got != 0 {
		t.Fatalf("anti-entropy record was stored during pause: %d versions", got)
	}

	s.resumeCoordinatorWrites()

	if _, err := s.ApplyReplicaRecord(ctx, replicaRequest); err != nil {
		t.Fatalf("replica apply after resume: %v", err)
	}
	if _, err := s.applyAntiEntropyBucket(bucketResponse, bucket); err != nil {
		t.Fatalf("anti-entropy apply after resume: %v", err)
	}
	if got := len(s.store.GetRecords(1)); got != 1 {
		t.Fatalf("replica versions after resume: got %d, want 1", got)
	}
	if got := len(s.store.GetRecords(2)); got != 1 {
		t.Fatalf("anti-entropy versions after resume: got %d, want 1", got)
	}
}
