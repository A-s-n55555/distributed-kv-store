package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPauseJoinPeerConfirmsRemotePause(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"
	firstAddress := startMembershipTestRPC(t, first)
	secondAddress := startMembershipTestRPC(t, second)
	peer := ring.Node{ID: "node-2", Address: secondAddress}

	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1", Address: firstAddress})
	clusterRing.AddNode(peer)

	current, err := membership.NewConfiguration(1, clusterRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("ab", 32)
	for _, s := range []*GRPCServer{first, second} {
		s.ring = clusterRing
		s.replicationFactor = 2
		if err := s.SetActiveMembership(current); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfigureJoinPause(
			filepath.Join(t.TempDir(), s.nodeID+"-pause.json"),
		); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfigureMembershipControlToken(token); err != nil {
			t.Fatal(err)
		}
	}

	candidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "127.0.0.1:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	unknown := ring.Node{ID: "node-4", Address: secondAddress}
	if _, err := first.pauseJoinPeer(ctx, unknown, current, candidate); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown destination: got %v, want InvalidArgument", err)
	}

	response, err := first.pauseJoinPeer(ctx, peer, current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetWritesPaused() ||
		response.GetPendingEpoch() != candidate.Identity().Epoch {
		t.Fatalf("remote pause not confirmed: %v", response)
	}
	if _, err := first.pauseJoinPeer(ctx, peer, current, candidate); err != nil {
		t.Fatalf("retry of remote pause: %v", err)
	}

	local, err := first.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if local.GetWritesPaused() {
		t.Fatal("pausing node-2 also paused node-1")
	}
	// Neither outcome is allowed while the coordinator is undecided.
	if _, err := first.commitJoinPeer(ctx, peer, current, candidate); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("remote commit without decision: %v", err)
	}
	if _, err := first.abortJoinPeer(ctx, peer, current, candidate); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("remote abort without decision: %v", err)
	}

	if err := first.recordJoinAbortDecision(current, candidate); err != nil {
		t.Fatal(err)
	}

	aborted, err := first.abortJoinPeer(ctx, peer, current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if aborted.GetWritesPaused() || aborted.GetPendingEpoch() != 0 {
		t.Fatalf("remote node remained paused: %v", aborted)
	}
	if _, err := first.abortJoinPeer(ctx, peer, current, candidate); err != nil {
		t.Fatalf("retry of remote abort: %v", err)
	}

	// A committed coordinator must prevent an abort on a peer that
	// has not yet recorded the commitment.
	commitCandidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-4", Address: "127.0.0.1:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.PauseForJoin(commitCandidate); err != nil {
		t.Fatal(err)
	}
	if _, err := first.pauseJoinPeer(ctx, peer, current, commitCandidate); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitForJoin(commitCandidate); err != nil {
		t.Fatal(err)
	}

	if _, err := first.abortJoinPeer(ctx, peer, current, commitCandidate); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("remote abort after coordinator commit: %v", err)
	}

	remote, err := second.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !confirmsJoinPause(
		remote, peer.ID, current.Identity(), commitCandidate.Identity(),
	) || remote.GetJoinCommitted() {
		t.Fatalf("rejected abort changed the pending peer: %v", remote)
	}

	for attempt := 0; attempt < 2; attempt++ {
		committed, err := first.commitJoinPeer(ctx, peer, current, commitCandidate)
		if err != nil {
			t.Fatalf("remote commit attempt %d: %v", attempt, err)
		}
		if !confirmsJoinCommit(
			committed, peer.ID, current.Identity(), commitCandidate.Identity(),
		) {
			t.Fatalf("remote commitment not confirmed: %v", committed)
		}
	}
}
