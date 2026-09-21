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

func TestPrepareJoinPauseRPCChecksAuthorizationAndIdentity(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"
	s.ring = ring.New(16)
	s.ring.AddNode(ring.Node{ID: "node-1", Address: "127.0.0.1:50051"})
	s.ring.AddNode(ring.Node{ID: "node-2", Address: "127.0.0.1:50052"})
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

	token := strings.Repeat("ab", 32)
	if err := s.ConfigureMembershipControlToken(token); err != nil {
		t.Fatal(err)
	}

	candidate, err := membership.PrepareJoin(active, ring.Node{
		ID: "node-3", Address: "127.0.0.1:50053",
	})
	if err != nil {
		t.Fatal(err)
	}
	activeID, candidateID := active.Identity(), candidate.Identity()
	request := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        activeID.Epoch,
		ActiveDigest:       append([]byte(nil), activeID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
		JoiningNodeId:      "node-3",
		JoiningNodeAddress: "127.0.0.1:50053",
	}

	address := startMembershipTestRPC(t, s)
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := kvpb.NewKeyValueStoreClient(connection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.PrepareJoinPause(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("request without token: got %v, want PermissionDenied", err)
	}

	authorized := metadata.AppendToOutgoingContext(
		ctx, membershipControlTokenHeader, token,
	)
	badDigest := append([]byte(nil), candidateID.Digest[:]...)
	badDigest[0] ^= 0xff
	badRequest := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        activeID.Epoch,
		ActiveDigest:       append([]byte(nil), activeID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    badDigest,
		JoiningNodeId:      "node-3",
		JoiningNodeAddress: "127.0.0.1:50053",
	}
	if _, err := client.PrepareJoinPause(authorized, badRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong candidate: got %v, want FailedPrecondition", err)
	}

	response, err := client.PrepareJoinPause(authorized, request)
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetWritesPaused() ||
		response.GetPendingEpoch() != candidateID.Epoch {
		t.Fatalf("node did not enter the requested pause: %v", response)
	}

	// Repeating the same request must be safe after an uncertain RPC result.
	if _, err := client.PrepareJoinPause(authorized, request); err != nil {
		t.Fatalf("retry of same pause: %v", err)
	}

	if _, err := client.AbortJoinPause(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("abort without token: got %v, want PermissionDenied", err)
	}
	if _, err := client.AbortJoinPause(authorized, badRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abort with wrong candidate: got %v, want FailedPrecondition", err)
	}

	aborted, err := client.AbortJoinPause(authorized, request)
	if err != nil {
		t.Fatal(err)
	}
	if aborted.GetWritesPaused() || aborted.GetPendingEpoch() != 0 {
		t.Fatalf("node remained paused after abort: %v", aborted)
	}

	// A lost response must not make retrying the same abort fail.
	again, err := client.AbortJoinPause(authorized, request)
	if err != nil {
		t.Fatalf("retry of abort: %v", err)
	}
	if again.GetWritesPaused() || again.GetPendingEpoch() != 0 {
		t.Fatalf("node paused after abort retry: %v", again)
	}
}
