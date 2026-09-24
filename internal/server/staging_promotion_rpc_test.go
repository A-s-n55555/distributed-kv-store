package server

import (
	"context"
	"net"
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

func TestStagingPromotionRPCAndRecovery(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"

	firstAddress := startMembershipTestRPC(t, first)
	secondAddress := startMembershipTestRPC(t, second)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: "node-1", Address: firstAddress})
	oldRing.AddNode(ring.Node{ID: "node-2", Address: secondAddress})

	current, err := membership.NewConfiguration(1, oldRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	joining := ring.Node{
		ID: "node-3", Address: listener.Addr().String(),
	}
	candidate, err := membership.PrepareJoin(current, joining)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare committed old members; leave them unactivated initially.
	for _, service := range []*GRPCServer{first, second} {
		service.ring = current.Ring()
		service.replicationFactor = 2
		service.readQuorum = 1
		service.writeQuorum = 2

		if err := service.ConfigureMembershipControlToken(
			strings.Repeat("ab", 32),
		); err != nil {
			t.Fatal(err)
		}

		if err := service.SetActiveMembership(current); err != nil {
			t.Fatal(err)
		}
		directory := t.TempDir()
		if err := membership.SaveCandidate(
			filepath.Join(directory, "membership-active.json"), current,
		); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureJoinPause(
			filepath.Join(directory, "membership-pause.json"),
		); err != nil {
			t.Fatal(err)
		}
		if err := service.PauseForJoin(candidate); err != nil {
			t.Fatal(err)
		}
		if err := service.CommitForJoin(candidate); err != nil {
			t.Fatal(err)
		}
	}

	token := strings.Repeat("ab", 32)
	pausePath := filepath.Join(t.TempDir(), "membership-pause.json")

	newStaging := func() *StagingService {
		t.Helper()

		target := newRecordTestServer(t)
		target.nodeID = joining.ID
		target.ring = candidate.Ring()
		target.replicationFactor = 2
		target.readQuorum = 1
		target.writeQuorum = 2

		if err := target.ConfigureMembershipControlToken(token); err != nil {
			t.Fatal(err)
		}
		staged, err := NewStagingService(target)
		if err != nil {
			t.Fatal(err)
		}
		if err := staged.ConfigureJoin(current, candidate); err != nil {
			t.Fatal(err)
		}
		if err := staged.ConfigurePromotion(pausePath); err != nil {
			t.Fatal(err)
		}
		return staged
	}

	staged := newStaging()
	rpcServer := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(rpcServer, staged)
	t.Cleanup(rpcServer.Stop)
	go func() { _ = rpcServer.Serve(listener) }()

	connection, err := grpc.NewClient(
		joining.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := kvpb.NewKeyValueStoreClient(connection)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	authorized := metadata.AppendToOutgoingContext(
		ctx, membershipControlTokenHeader, token,
	)

	oldID, candidateID := current.Identity(), candidate.Identity()
	request := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        oldID.Epoch,
		ActiveDigest:       append([]byte(nil), oldID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
		JoiningNodeId:      joining.ID,
		JoiningNodeAddress: joining.Address,
	}

	if _, err := client.PromoteJoin(
		ctx, request,
	); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unauthorized promotion: got %v, want PermissionDenied", err)
	}

	if _, err := client.PromoteJoin(
		authorized, request,
	); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("premature promotion: got %v, want FailedPrecondition", err)
	}

	for _, service := range []*GRPCServer{first, second} {
		if err := service.ActivateForJoin(candidate); err != nil {
			t.Fatal(err)
		}
	}

	for attempt := 0; attempt < 2; attempt++ {
		response, err := client.PromoteJoin(authorized, request)
		if err != nil {
			t.Fatalf("promotion attempt %d: %v", attempt+1, err)
		}
		if response.GetNodeId() != joining.ID ||
			!response.GetPromotionRecorded() ||
			!response.GetRequestsPaused() {
			t.Fatalf("unexpected promotion response: %v", response)
		}
	}

	response, err := client.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !confirmsJoinActivation(response, joining.ID, candidateID) {
		t.Fatalf("unexpected promoted membership status: %v", response)
	}

	if _, err := client.Put(
		ctx, &kvpb.PutRequest{Key: 1, Value: "blocked"},
	); status.Code(err) != codes.Unavailable {
		t.Fatalf("promoted Put: got %v, want Unavailable", err)
	}

	// Enable replica updates across all three members, then retry.
	for attempt := 0; attempt < 2; attempt++ {
		if err := first.enableAllJoinReplicas(ctx, candidate); err != nil {
			t.Fatalf("replica readiness attempt %d: %v", attempt+1, err)
		}
	}

	for _, service := range []*GRPCServer{first, second, staged.target} {
		observed, err := service.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !confirmsJoinReplicaReady(
			observed, service.nodeID, candidateID,
		) {
			t.Fatalf("%s is not replica-ready: %v", service.nodeID, observed)
		}

		// Client requests must remain blocked.
		if _, err := service.Put(
			ctx, &kvpb.PutRequest{Key: 7, Value: "blocked"},
		); status.Code(err) != codes.Unavailable {
			t.Fatalf("%s client Put: got %v, want Unavailable", service.nodeID, err)
		}

		// Replica writes must now be accepted.
		if _, err := service.ApplyReplicaRecord(
			ctx,
			&kvpb.ReplicaRecordRequest{
				Key: 7,
				Record: &kvpb.VersionedRecord{
					Value: "replica-ready",
					Clock: map[string]uint64{"node-1": 1},
				},
			},
		); err != nil {
			t.Fatalf("%s replica apply: %v", service.nodeID, err)
		}
	}

	// Replica readiness alone must not authorize client release.
	if _, err := client.ReleaseJoinClients(
		authorized, request,
	); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("release without authorization: got %v, want FailedPrecondition", err)
	}

	if err := first.prepareJoinClientRelease(ctx, candidate); err != nil {
		t.Fatalf("authorize client release: %v", err)
	}

	// Simulate interruption after only node-3 released clients.
	releasedStatus, err := client.ReleaseJoinClients(authorized, request)
	if err != nil {
		t.Fatalf("release node-3: %v", err)
	}
	if !confirmsJoinClientsReleased(releasedStatus, joining.ID, candidateID) {
		t.Fatalf("node-3 did not release clients: %v", releasedStatus)
	}

	for _, service := range []*GRPCServer{first, second} {
		observed, err := service.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !confirmsJoinReplicaReady(observed, service.nodeID, candidateID) {
			t.Fatalf("%s unexpectedly left the replica-ready phase", service.nodeID)
		}
	}

	coordinatorConnection, err := grpc.NewClient(
		firstAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinatorConnection.Close()

	coordinatorClient := kvpb.NewKeyValueStoreClient(coordinatorConnection)

	for attempt := 0; attempt < 2; attempt++ {
		observed, err := coordinatorClient.CoordinateJoinResume(
			authorized, request,
		)
		if err != nil {
			t.Fatalf("resume RPC attempt %d: %v", attempt+1, err)
		}
		if !confirmsJoinClientsReleased(
			observed, first.nodeID, candidateID,
		) {
			t.Fatalf("coordinator did not confirm release: %v", observed)
		}
	}

	for _, service := range []*GRPCServer{first, second, staged.target} {
		observed, err := service.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !confirmsJoinClientsReleased(observed, service.nodeID, candidateID) {
			t.Fatalf("%s is not released: %v", service.nodeID, observed)
		}
	}

	// Exercise normal client traffic through promoted node-3.
	if _, err := client.Put(ctx, &kvpb.PutRequest{
		Key: 900, Value: "after-resume",
	}); err != nil {
		t.Fatalf("Put after resume: %v", err)
	}

	value, err := client.Get(ctx, &kvpb.GetRequest{Key: 900})
	if err != nil {
		t.Fatalf("Get after resume: %v", err)
	}
	if !value.GetFound() || value.GetValue() != "after-resume" {
		t.Fatalf("unexpected value after resume: %v", value)
	}

	// Reconstruct membership state from disk.
	// newStaging creates a fresh test store, so this is not a WAL recovery test.
	rpcServer.Stop()
	recovered := newStaging()

	response, err = recovered.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !confirmsJoinClientsReleased(response, joining.ID, candidateID) {
		t.Fatalf("recovery lost client release: %v", response)
	}

	// Serve the recovered node at its original membership address.
	recoveredListener, err := net.Listen("tcp", joining.Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recoveredListener.Close() })

	recoveredRPC := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(recoveredRPC, recovered)
	t.Cleanup(recoveredRPC.Stop)
	go func() { _ = recoveredRPC.Serve(recoveredListener) }()

	recoveredConnection, err := grpc.NewClient(
		joining.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recoveredConnection.Close() })
	recoveredClient := kvpb.NewKeyValueStoreClient(recoveredConnection)

	// Retrying the completed phase must also accept the recovered member.
	if _, err := coordinatorClient.CoordinateJoinResume(
		authorized, request,
	); err != nil {
		t.Fatalf("resume RPC retry after recovery: %v", err)
	}
	// Verify fresh client traffic after membership recovery.
	if _, err := recoveredClient.Put(
		ctx,
		&kvpb.PutRequest{Key: 901, Value: "after-recovery"},
		grpc.WaitForReady(true),
	); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}

	recoveredValue, err := recoveredClient.Get(
		ctx, &kvpb.GetRequest{Key: 901},
	)
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if !recoveredValue.GetFound() ||
		recoveredValue.GetValue() != "after-recovery" {
		t.Fatalf("unexpected recovered-node value: %v", recoveredValue)
	}
}
