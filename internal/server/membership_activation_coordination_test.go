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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestActivateOldMembersRetriesPartialActivation(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"

	firstAddress := startMembershipTestRPC(t, first)
	secondAddress := startMembershipTestRPC(t, second)

	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: first.nodeID, Address: firstAddress})
	oldRing.AddNode(ring.Node{ID: second.nodeID, Address: secondAddress})

	current, err := membership.NewConfiguration(1, oldRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := membership.PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "127.0.0.1:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, service := range []*GRPCServer{first, second} {
		// Each simulated process must have its own ring.
		service.ring = current.Ring()
		service.replicationFactor = 2

		if err := service.SetActiveMembership(current); err != nil {
			t.Fatal(err)
		}

		directory := t.TempDir()
		if err := membership.SaveCandidate(
			filepath.Join(directory, "membership-active.json"),
			current,
		); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureJoinPause(
			filepath.Join(directory, "membership-pause.json"),
		); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureMembershipControlToken(
			strings.Repeat("ab", 32),
		); err != nil {
			t.Fatal(err)
		}
		if err := service.PauseForJoin(candidate); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := first.CommitForJoin(candidate); err != nil {
		t.Fatal(err)
	}

	// Refuse activation while an old member has not committed.
	if err := first.activateOldMembersForJoin(
		ctx, candidate,
	); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("uncommitted peer: got %v, want FailedPrecondition", err)
	}
	if first.ring.NodeCount() != 2 {
		t.Fatal("coordinator activated before every old member committed")
	}

	if err := second.CommitForJoin(candidate); err != nil {
		t.Fatal(err)
	}

	// Simulate interruption after only the coordinator activated.
	if err := first.ActivateForJoin(candidate); err != nil {
		t.Fatal(err)
	}

	connection, err := grpc.NewClient(
		firstAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	oldID, candidateID := current.Identity(), candidate.Identity()
	request := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        oldID.Epoch,
		ActiveDigest:       append([]byte(nil), oldID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
		JoiningNodeId:      "node-3",
		JoiningNodeAddress: "127.0.0.1:50053",
	}
	client := kvpb.NewKeyValueStoreClient(connection)

	if _, err := client.CoordinateJoinActivation(
		ctx, request,
	); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unauthorized activation: got %v, want PermissionDenied", err)
	}

	authorized := metadata.AppendToOutgoingContext(
		ctx, membershipControlTokenHeader, strings.Repeat("ab", 32),
	)
	for attempt := 0; attempt < 2; attempt++ {
		response, err := client.CoordinateJoinActivation(authorized, request)
		if err != nil {
			t.Fatalf("activation RPC attempt %d: %v", attempt+1, err)
		}
		if !confirmsJoinActivation(response, first.nodeID, candidateID) {
			t.Fatalf("coordinator did not confirm activation: %v", response)
		}
	}

	for _, service := range []*GRPCServer{first, second} {
		response, err := service.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !confirmsJoinActivation(
			response, service.nodeID, candidate.Identity(),
		) {
			t.Fatalf("%s did not remain activated and paused", service.nodeID)
		}
		if service.ring.NodeCount() != 3 {
			t.Fatalf("%s still has the old ring", service.nodeID)
		}
	}
}
