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

func TestPauseOldMembersForJoin(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	first.nodeID = "node-1"
	second.nodeID = "node-2"

	secondAddress := startMembershipTestRPC(t, second)
	clusterRing := ring.New(16)
	clusterRing.AddNode(ring.Node{ID: "node-1", Address: "127.0.0.1:50051"})
	clusterRing.AddNode(ring.Node{ID: "node-2", Address: secondAddress})

	current, err := membership.NewConfiguration(1, clusterRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("ab", 32)
	for _, service := range []*GRPCServer{first, second} {
		service.ring = clusterRing
		service.replicationFactor = 2
		if err := service.SetActiveMembership(current); err != nil {
			t.Fatal(err)
		}
		if err := service.ConfigureJoinPause(
			filepath.Join(t.TempDir(), service.nodeID+"-pause.json"),
		); err != nil {
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

	if err := first.pauseOldMembersForJoin(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	if err := first.pauseOldMembersForJoin(ctx, candidate); err != nil {
		t.Fatalf("retry of cluster pause: %v", err)
	}

	for _, service := range []*GRPCServer{first, second} {
		response, err := service.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !confirmsJoinPause(
			response, service.nodeID, current.Identity(), candidate.Identity(),
		) {
			t.Fatalf("%s has incorrect pause: %v", service.nodeID, response)
		}
		_, err = service.Get(ctx, &kvpb.GetRequest{Key: 99})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("%s allowed a client read while paused: %v", service.nodeID, err)
		}
	}
}
