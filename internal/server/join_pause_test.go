package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPauseForJoinChecksCandidateAndResumeIdentity(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"
	s.ring = ring.New(16)
	s.ring.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	s.ring.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})
	s.replicationFactor = 2

	active, err := membership.NewConfiguration(1, s.ring, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetActiveMembership(active); err != nil {
		t.Fatal(err)
	}

	pausePath := filepath.Join(t.TempDir(), "membership-pause.json")
	if err := s.ConfigureJoinPause(pausePath); err != nil {
		t.Fatal(err)
	}

	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.PauseForJoin(active); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unchanged candidate: got %v, want FailedPrecondition", err)
	}
	if err := s.PauseForJoin(candidate); err != nil {
		t.Fatal(err)
	}
	if err := s.PauseForJoin(candidate); err != nil {
		t.Fatalf("retry of same pause: %v", err)
	}

	request := &kvpb.ReplicaRecordRequest{
		Key: 1,
		Record: &kvpb.VersionedRecord{
			Value: "value",
			Clock: map[string]uint64{"node-1": 1},
		},
	}
	if _, err := s.ApplyReplicaRecord(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("replica apply while paused: got %v, want Unavailable", err)
	}
	if err := s.ResumeForJoin(active.Identity()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resume with old identity: got %v, want FailedPrecondition", err)
	}
	if err := s.ResumeForJoin(candidate.Identity()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyReplicaRecord(context.Background(), request); err != nil {
		t.Fatalf("replica apply after resume: %v", err)
	}
}

// ClearJoinPause removes only the marker for the expected candidate.

func TestJoinPauseIsRestoredBeforeServing(t *testing.T) {
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	clusterRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

	active, err := membership.NewConfiguration(1, clusterRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "membership-pause.json")

	newNode := func() *GRPCServer {
		t.Helper()
		s := newRecordTestServer(t)
		s.nodeID = "node-1"
		s.ring = clusterRing
		s.replicationFactor = 2
		if err := s.SetActiveMembership(active); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfigureJoinPause(path); err != nil {
			t.Fatal(err)
		}
		return s
	}

	first := newNode()
	if err := first.PauseForJoin(candidate); err != nil {
		t.Fatal(err)
	}

	// Constructing a fresh server simulates restart with the same marker.
	restarted := newNode()
	request := &kvpb.ReplicaRecordRequest{
		Key: 1,
		Record: &kvpb.VersionedRecord{
			Value: "value",
			Clock: map[string]uint64{"node-1": 1},
		},
	}
	if _, err := restarted.ApplyReplicaRecord(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("replica apply after restart: got %v, want Unavailable", err)
	}

	if err := restarted.ResumeForJoin(candidate.Identity()); err != nil {
		t.Fatal(err)
	}
	afterResume := newNode()
	if _, err := afterResume.ApplyReplicaRecord(context.Background(), request); err != nil {
		t.Fatalf("replica apply after durable resume: %v", err)
	}
}

func TestCommittedJoinStaysPausedAfterRestart(t *testing.T) {
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	clusterRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

	active, err := membership.NewConfiguration(1, clusterRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	pausePath := filepath.Join(t.TempDir(), "membership-pause.json")

	newNode := func() *GRPCServer {
		t.Helper()
		s := newRecordTestServer(t)
		s.nodeID = "node-1"
		s.ring = clusterRing
		s.replicationFactor = 2
		if err := s.SetActiveMembership(active); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfigureJoinPause(pausePath); err != nil {
			t.Fatal(err)
		}
		return s
	}

	first := newNode()
	if err := first.PauseForJoin(candidate); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitForJoin(candidate); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitForJoin(candidate); err != nil {
		t.Fatalf("retry commit: %v", err)
	}

	restarted := newNode()
	if err := restarted.AbortForJoin(candidate); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abort after restart: got %v, want FailedPrecondition", err)
	}
	response, err := restarted.GetMembershipStatus(
		context.Background(), &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetWritesPaused() {
		t.Fatal("committed join lost its pause after restart")
	}
	if !response.GetJoinCommitted() {
		t.Fatal("restart lost the join commitment")
	}
}
