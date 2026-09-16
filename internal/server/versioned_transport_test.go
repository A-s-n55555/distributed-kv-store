package server

import (
	"context"
	"net"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func TestVersionedTransportOverGRPC(t *testing.T) {
	replica := newRecordTestServer(t)
	replica.nodeID = "replica-1"

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(grpcServer, replica)

	t.Cleanup(func() {
		grpcServer.Stop()
	})

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	coordinator := &GRPCServer{
		nodeID: "coordinator",
	}

	target := ring.Node{
		ID:      "replica-1",
		Address: listener.Addr().String(),
	}

	ctx := context.Background()

	valueRecord := store.Record{
		Value: "network-value",
		Clock: version.Clock{"coordinator": 1},
	}

	if err := coordinator.applyRecordToReplica(
		ctx,
		target,
		1,
		valueRecord,
	); err != nil {
		t.Fatalf("remote apply error = %v", err)
	}

	got, exists, err := coordinator.readRecordFromReplica(
		ctx,
		target,
		1,
	)
	if err != nil {
		t.Fatalf("remote read error = %v", err)
	}

	if !exists ||
		got.Deleted ||
		got.Value != "network-value" ||
		got.Clock["coordinator"] != 1 {
		t.Fatalf("incorrect remote record: %+v, exists=%v", got, exists)
	}

	tombstone := store.Record{
		Deleted: true,
		Clock:   version.Clock{"coordinator": 2},
	}

	if err := coordinator.applyRecordToReplica(
		ctx,
		target,
		1,
		tombstone,
	); err != nil {
		t.Fatalf("remote tombstone apply error = %v", err)
	}

	got, exists, err = coordinator.readRecordFromReplica(
		ctx,
		target,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !exists || !got.Deleted || got.Clock["coordinator"] != 2 {
		t.Fatalf("incorrect remote tombstone: %+v", got)
	}

	_, exists, err = coordinator.readRecordFromReplica(
		ctx,
		target,
		99,
	)
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("missing key was reported as existing")
	}
}
