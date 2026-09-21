package server

import (
	"bytes"
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

func TestGetJoinDecisionRPC(t *testing.T) {
	for _, outcome := range []string{"commit", "abort"} {
		t.Run(outcome, func(t *testing.T) {
			s := newRecordTestServer(t)
			s.nodeID = "node-1"
			s.ring = ring.New(16)
			s.ring.AddNode(ring.Node{
				ID: "node-1", Address: "127.0.0.1:50051",
			})
			s.ring.AddNode(ring.Node{
				ID: "node-2", Address: "127.0.0.1:50052",
			})
			s.replicationFactor = 2

			current, err := membership.NewConfiguration(1, s.ring, 2)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SetActiveMembership(current); err != nil {
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

			candidate, err := membership.PrepareJoin(current, ring.Node{
				ID: "node-3", Address: "127.0.0.1:50053",
			})
			if err != nil {
				t.Fatal(err)
			}
			activeID, candidateID := current.Identity(), candidate.Identity()
			request := &kvpb.PrepareJoinPauseRequest{
				ActiveEpoch:        activeID.Epoch,
				ActiveDigest:       append([]byte(nil), activeID.Digest[:]...),
				CandidateEpoch:     candidateID.Epoch,
				CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
				JoiningNodeId:      "node-3",
				JoiningNodeAddress: "127.0.0.1:50053",
			}

			conn, err := grpc.NewClient(
				startMembershipTestRPC(t, s),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := kvpb.NewKeyValueStoreClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.GetJoinDecision(ctx, request); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("query without token: %v", err)
			}
			authorized := metadata.AppendToOutgoingContext(
				ctx, membershipControlTokenHeader, token,
			)

			response, err := client.GetJoinDecision(authorized, request)
			if err != nil {
				t.Fatal(err)
			}
			if response.GetDecision() != kvpb.JoinDecision_JOIN_DECISION_UNDECIDED {
				t.Fatalf("missing decision: got %v", response)
			}

			local, err := s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if local.GetWritesPaused() {
				t.Fatal("read-only query paused the node")
			}

			if err := s.PauseForJoin(candidate); err != nil {
				t.Fatal(err)
			}
			want := kvpb.JoinDecision_JOIN_DECISION_COMMIT
			if outcome == "commit" {
				err = s.CommitForJoin(candidate)
			} else {
				want = kvpb.JoinDecision_JOIN_DECISION_ABORT
				err = s.recordJoinAbortDecision(current, candidate)
			}
			if err != nil {
				t.Fatal(err)
			}

			response, err = client.GetJoinDecision(authorized, request)
			if err != nil {
				t.Fatal(err)
			}
			if response.GetCoordinatorNodeId() != "node-1" ||
				response.GetActiveEpoch() != activeID.Epoch ||
				!bytes.Equal(response.GetActiveDigest(), activeID.Digest[:]) ||
				response.GetCandidateEpoch() != candidateID.Epoch ||
				!bytes.Equal(response.GetCandidateDigest(), candidateID.Digest[:]) ||
				response.GetDecision() != want {
				t.Fatalf("incorrect decision response: %v", response)
			}

			local, err = s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if !confirmsJoinPause(local, s.nodeID, activeID, candidateID) {
				t.Fatalf("decision query changed the pause: %v", local)
			}

			request.CandidateDigest[0] ^= 0xff
			if _, err := client.GetJoinDecision(authorized, request); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("query with wrong candidate: %v", err)
			}
		})
	}
}
