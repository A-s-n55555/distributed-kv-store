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

func TestJoinDecisionRejectsOtherCoordinator(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-2"
	s.ring = ring.New(16)
	s.ring.AddNode(ring.Node{
		ID: "node-2", Address: "127.0.0.1:50052",
	})
	s.ring.AddNode(ring.Node{
		ID: "node-1", Address: "127.0.0.1:50051",
	})
	s.replicationFactor = 2

	current, err := membership.NewConfiguration(1, s.ring, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetActiveMembership(current); err != nil {
		t.Fatal(err)
	}

	pausePath := filepath.Join(t.TempDir(), "membership-pause.json")
	if err := s.ConfigureJoinPause(pausePath); err != nil {
		t.Fatal(err)
	}

	candidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "127.0.0.1:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PauseForJoin(candidate); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for _, operation := range []struct {
		name string
		run  func(context.Context, membership.Configuration) error
	}{
		{"commit", s.commitOldMembersForJoin},
		{"abort", s.abortOldMembersForJoin},
	} {
		err := operation.run(ctx, candidate)
		if status.Code(err) != codes.FailedPrecondition ||
			!strings.Contains(status.Convert(err).Message(), "coordinated by node-1") {
			t.Fatalf("%s: expected coordinator rejection, got %v",
				operation.name, err)
		}
	}

	response, err := s.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !confirmsJoinPause(
		response, s.nodeID, current.Identity(), candidate.Identity(),
	) || response.GetJoinCommitted() {
		t.Fatalf("rejected operations changed join state: %v", response)
	}

	aborted, err := membership.JoinWasAborted(
		pausePath, current, candidate,
	)
	if err != nil || aborted {
		t.Fatalf("rejected abort wrote a decision: aborted=%v, err=%v",
			aborted, err)
	}
}
