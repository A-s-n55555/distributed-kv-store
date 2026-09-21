package server

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

func TestAbortOldMembersAfterPartialAndCompletePause(t *testing.T) {
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
	var secondPausePath string
	for _, service := range []*GRPCServer{first, second} {
		service.ring = clusterRing
		service.replicationFactor = 2
		if err := service.SetActiveMembership(current); err != nil {
			t.Fatal(err)
		}
		pausePath := filepath.Join(t.TempDir(), service.nodeID+"-pause.json")
		if service == second {
			secondPausePath = pausePath
		}
		if err := service.ConfigureJoinPause(pausePath); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureMembershipControlToken(token); err != nil {
			t.Fatal(err)
		}
	}

	candidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "127.0.0.1:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Simulate failure after only the remote node was paused.
	if _, err := first.pauseJoinPeer(ctx, peer, current, candidate); err != nil {
		t.Fatal(err)
	}

	failedCtx, cancelFailed := context.WithCancel(ctx)
	cancelFailed()
	if err := first.abortOldMembersForJoin(failedCtx, candidate); status.Code(err) != codes.Unavailable {
		t.Fatalf("interrupted remote abort: got %v, want Unavailable", err)
	}

	aborted, err := membership.JoinWasAborted(
		first.joinPausePath, current, candidate,
	)
	if err != nil || !aborted {
		t.Fatalf("abort decision was not saved: aborted=%v, err=%v", aborted, err)
	}

	if err := first.abortOldMembersForJoin(ctx, candidate); err != nil {
		t.Fatalf("abort partial pause: %v", err)
	}

	restartedSecond := newRecordTestServer(t)
	restartedSecond.nodeID = "node-2"
	restartedSecond.ring = clusterRing
	restartedSecond.replicationFactor = 2

	if err := restartedSecond.SetActiveMembership(current); err != nil {
		t.Fatal(err)
	}
	if err := restartedSecond.ConfigureJoinPause(secondPausePath); err != nil {
		t.Fatal(err)
	}
	if err := restartedSecond.PauseForJoin(candidate); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("restarted node accepted aborted candidate: %v", err)
	}

	// Both nodes must reject a late pause for the aborted candidate.
	for _, service := range []*GRPCServer{first, second} {
		if err := service.PauseForJoin(candidate); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("%s accepted aborted candidate: %v", service.nodeID, err)
		}
	}

	// Use a distinct candidate to test a complete pause and abort.
	nextCandidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-4", Address: "127.0.0.1:50054",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.pauseOldMembersForJoin(ctx, nextCandidate); err != nil {
		t.Fatal(err)
	}
	if err := first.abortOldMembersForJoin(ctx, nextCandidate); err != nil {
		t.Fatalf("abort complete pause: %v", err)
	}
	if err := first.abortOldMembersForJoin(ctx, nextCandidate); err != nil {
		t.Fatalf("retry abort: %v", err)
	}

	for _, service := range []*GRPCServer{first, second} {
		response, err := service.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !confirmsJoinAbort(
			response, service.nodeID, current.Identity(),
		) {
			t.Fatalf("%s did not resume: %v", service.nodeID, response)
		}
	}
	// A local commitment must prevent the coordinator from aborting
	// the still-uncommitted remote member.
	committedCandidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-5", Address: "127.0.0.1:50055",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.pauseOldMembersForJoin(ctx, committedCandidate); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitForJoin(committedCandidate); err != nil {
		t.Fatal(err)
	}
	if err := first.abortOldMembersForJoin(ctx, committedCandidate); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abort after local commit: got %v, want FailedPrecondition", err)
	}

	remoteStatus, err := second.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !confirmsJoinPause(
		remoteStatus, second.nodeID,
		current.Identity(), committedCandidate.Identity(),
	) {
		t.Fatalf("abort changed remote pause: %v", remoteStatus)
	}
}
