package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

func TestGetMembershipStatusTracksJoinPause(t *testing.T) {
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
	if err := s.ConfigureJoinPause(
		filepath.Join(t.TempDir(), "membership-pause.json"),
	); err != nil {
		t.Fatal(err)
	}

	readStatus := func() *kvpb.MembershipStatusResponse {
		t.Helper()
		response, err := s.GetMembershipStatus(
			context.Background(), &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}

	activeID := active.Identity()
	before := readStatus()
	if before.GetNodeId() != "node-1" ||
		before.GetActiveEpoch() != activeID.Epoch ||
		!bytes.Equal(before.GetActiveDigest(), activeID.Digest[:]) ||
		before.GetWritesPaused() || before.GetPendingEpoch() != 0 {
		t.Fatalf("incorrect active status: %v", before)
	}

	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PauseForJoin(candidate); err != nil {
		t.Fatal(err)
	}

	pendingID := candidate.Identity()
	during := readStatus()
	if !during.GetWritesPaused() ||
		during.GetPendingEpoch() != pendingID.Epoch ||
		!bytes.Equal(during.GetPendingDigest(), pendingID.Digest[:]) {
		t.Fatalf("incorrect paused status: %v", during)
	}

	if err := s.ResumeForJoin(pendingID); err != nil {
		t.Fatal(err)
	}
	after := readStatus()
	if after.GetWritesPaused() ||
		after.GetPendingEpoch() != 0 ||
		len(after.GetPendingDigest()) != 0 {
		t.Fatalf("incorrect resumed status: %v", after)
	}
}
