package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestActivatedJoinRecoveryKeepsRequestsPaused(t *testing.T) {
	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{
		ID: "node-1", Address: "localhost:50051",
	})
	oldRing.AddNode(ring.Node{
		ID: "node-2", Address: "localhost:50052",
	})

	current, err := membership.NewConfiguration(1, oldRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	activePath := filepath.Join(directory, "membership-active.json")
	pausePath := filepath.Join(directory, "membership-pause.json")

	if err := membership.SaveCandidate(activePath, current); err != nil {
		t.Fatal(err)
	}
	if err := membership.SaveJoinPause(
		pausePath, current, candidate,
	); err != nil {
		t.Fatal(err)
	}
	if err := membership.RecordJoinCommit(
		pausePath, current, candidate,
	); err != nil {
		t.Fatal(err)
	}

	live := newRecordTestServer(t)
	live.nodeID = "node-1"
	live.ring = current.Ring()
	live.replicationFactor = 2

	if err := live.SetActiveMembership(current); err != nil {
		t.Fatal(err)
	}
	if err := live.ConfigureJoinPause(pausePath); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := live.ActivateForJoin(candidate); err != nil {
			t.Fatalf("live activation attempt %d: %v", attempt+1, err)
		}
	}

	liveStatus, err := live.GetMembershipStatus(
		context.Background(), &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	candidateID := candidate.Identity()
	if liveStatus.GetActiveEpoch() != candidateID.Epoch ||
		!bytes.Equal(liveStatus.GetActiveDigest(), candidateID.Digest[:]) ||
		!liveStatus.GetWritesPaused() ||
		!liveStatus.GetJoinCommitted() {
		t.Fatal("live activation did not preserve candidate identity and pause")
	}
	if live.ring.NodeCount() != 3 {
		t.Fatal("live ring does not contain the joining node")
	}

	if err := membership.ActivateCommittedJoin(
		activePath, pausePath, current, candidate,
	); err != nil {
		t.Fatal(err)
	}

	// Retrying the same durable activation must succeed.
	if err := membership.ActivateCommittedJoin(
		activePath, pausePath, current, candidate,
	); err != nil {
		t.Fatalf("retry activation: %v", err)
	}

	// Simulate startup with the original -nodes configuration.
	active, err := membership.LoadJoinAwareActive(
		activePath, pausePath, oldRing, 2, false,
	)
	if err != nil {
		t.Fatalf("load activated membership: %v", err)
	}
	if active.Identity() != candidate.Identity() {
		t.Fatal("restart did not load candidate membership")
	}

	newRecoveredServer := func() (*GRPCServer, error) {
		t.Helper()

		s := newRecordTestServer(t)
		s.nodeID = "node-1"
		s.ring = active.Ring()
		s.replicationFactor = 2

		if err := s.SetActiveMembership(active); err != nil {
			return nil, err
		}
		if err := s.ConfigureJoinPause(pausePath); err != nil {
			return nil, err
		}
		return s, nil
	}

	restarted, err := newRecoveredServer()
	if err != nil {
		t.Fatalf("restore activated pause: %v", err)
	}

	ctx := context.Background()
	response, err := restarted.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := candidate.Identity()
	if response.GetActiveEpoch() != expected.Epoch ||
		!bytes.Equal(response.GetActiveDigest(), expected.Digest[:]) {
		t.Fatal("status reports the wrong active membership")
	}
	if !response.GetWritesPaused() || !response.GetJoinCommitted() {
		t.Fatal("activated join lost its pause or commit")
	}

	assertUnavailable := func(operation string, err error) {
		t.Helper()
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("%s: got %v, want Unavailable", operation, err)
		}
	}

	_, err = restarted.Get(ctx, &kvpb.GetRequest{Key: 1})
	assertUnavailable("get", err)

	_, err = restarted.Put(ctx, &kvpb.PutRequest{
		Key: 1, Value: "blocked",
	})
	assertUnavailable("put", err)

	_, err = restarted.Delete(ctx, &kvpb.DeleteRequest{Key: 1})
	assertUnavailable("delete", err)

	_, err = restarted.ApplyReplicaRecord(ctx, &kvpb.ReplicaRecordRequest{
		Key: 1,
		Record: &kvpb.VersionedRecord{
			Value: "blocked",
			Clock: map[string]uint64{"node-1": 1},
		},
	})
	assertUnavailable("replica apply", err)

	// A missing pause must prevent startup after activation.
	if err := os.Remove(pausePath); err != nil {
		t.Fatal(err)
	}
	if _, err := newRecoveredServer(); err == nil {
		t.Fatal("startup accepted an activated join without its pause")
	}
}
