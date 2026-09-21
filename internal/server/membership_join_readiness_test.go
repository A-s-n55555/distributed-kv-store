package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestCheckJoinReadinessAcrossOldMembers(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"

	secondAddress := startMembershipTestRPC(t, second)
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1", Address: "127.0.0.1:50051"})
	clusterRing.AddNode(ring.Node{ID: "node-2", Address: secondAddress})

	active, err := membership.NewConfiguration(1, clusterRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*GRPCServer{first, second} {
		s.ring = clusterRing
		s.replicationFactor = 2
		if err := s.SetActiveMembership(active); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfigureJoinPause(
			filepath.Join(t.TempDir(), s.nodeID+"-pause.json"),
		); err != nil {
			t.Fatal(err)
		}
	}

	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "127.0.0.1:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := first.checkJoinReadiness(ctx, candidate); err != nil {
		t.Fatalf("matching old members: %v", err)
	}

	// A different active epoch must stop preparation.
	differentEpoch, err := membership.NewConfiguration(2, clusterRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.SetActiveMembership(differentEpoch); err != nil {
		t.Fatal(err)
	}
	if err := first.checkJoinReadiness(ctx, candidate); err == nil {
		t.Fatal("accepted old members with different active epochs")
	}
	if err := second.SetActiveMembership(active); err != nil {
		t.Fatal(err)
	}

	// A pause for another candidate must also stop preparation.
	other, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-4", Address: "127.0.0.1:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.PauseForJoin(other); err != nil {
		t.Fatal(err)
	}
	if err := first.checkJoinReadiness(ctx, candidate); err == nil {
		t.Fatal("accepted peer paused for another join")
	}
}
