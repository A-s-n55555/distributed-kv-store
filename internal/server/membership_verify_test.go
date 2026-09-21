package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func startMembershipTestRPC(t *testing.T, service *GRPCServer) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(grpcServer, service)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-done
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func TestVerifyPreCopyFindsSiblingOnOtherOldOwner(t *testing.T) {
	first := newRecordTestServer(t)
	second := newRecordTestServer(t)
	target := newRecordTestServer(t)

	first.nodeID = "node-1"
	second.nodeID = "node-2"
	target.nodeID = "node-3"

	secondAddress := startMembershipTestRPC(t, second)
	targetAddress := startMembershipTestRPC(t, target)

	current := ring.New(32)
	proposed := ring.New(32)
	for _, node := range []ring.Node{
		{ID: "node-1", Address: "127.0.0.1:50051"},
		{ID: "node-2", Address: secondAddress},
	} {
		current.AddNode(node)
		proposed.AddNode(node)
	}
	proposed.AddNode(ring.Node{
		ID: "node-3", Address: targetAddress,
	})

	for _, service := range []*GRPCServer{first, second} {
		service.ring = current
		service.replicationFactor = 2
	}

	var key int64
	found := false
	for candidate := int64(0); candidate < 1000; candidate++ {
		diff, err := ring.DiffReplicasForKey(
			current, proposed, candidate, 2,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) == 1 && diff.Added[0].ID == "node-3" {
			key = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no key moved to node-3")
	}

	live := store.Record{
		Value: "live",
		Clock: version.Clock{"node-1": 1},
	}
	tombstone := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}
	if err := first.store.ApplyRecord(key, live); err != nil {
		t.Fatal(err)
	}
	if err := second.store.ApplyRecord(key, tombstone); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := first.preCopyKey(ctx, proposed, key); err != nil {
		t.Fatal(err)
	}
	if err := first.verifyPreCopyKey(ctx, proposed, key); err == nil {
		t.Fatal("verification passed despite a missing tombstone sibling")
	}

	if _, err := second.preCopyKey(ctx, proposed, key); err != nil {
		t.Fatal(err)
	}
	if err := first.verifyPreCopyKey(ctx, proposed, key); err != nil {
		t.Fatalf("verification failed after both copies: %v", err)
	}
}
