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

func TestPreCopyKeySendsSiblingsAndTombstone(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	target := newRecordTestServer(t)
	target.nodeID = "node-3"

	grpcServer := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(grpcServer, target)
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

	current := ring.New(32)
	proposed := ring.New(32)
	for _, node := range []ring.Node{
		{ID: "node-1", Address: "127.0.0.1:50051"},
		{ID: "node-2", Address: "127.0.0.1:50052"},
	} {
		current.AddNode(node)
		proposed.AddNode(node)
	}
	proposed.AddNode(ring.Node{
		ID: "node-3", Address: listener.Addr().String(),
	})

	source := newRecordTestServer(t)
	source.nodeID = "node-1"
	source.ring = current
	source.replicationFactor = 2

	var movedKey int64
	found := false
	for key := int64(0); key < 1000; key++ {
		diff, err := ring.DiffReplicasForKey(current, proposed, key, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) == 1 && diff.Added[0].ID == "node-3" {
			movedKey = key
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
	for _, record := range []store.Record{live, tombstone} {
		if err := source.store.ApplyRecord(movedKey, record); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sent, err := source.preCopyKey(ctx, proposed, movedKey)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 2 {
		t.Fatalf("sent %d versions; want 2", sent)
	}

	got := target.store.GetRecords(movedKey)
	if len(got) != 2 ||
		!containsRecord(got, live) ||
		!containsRecord(got, tombstone) {
		t.Fatalf("target is missing versions: %+v", got)
	}

	// Repeating the copy must not create extra siblings.
	if _, err := source.preCopyKey(ctx, proposed, movedKey); err != nil {
		t.Fatal(err)
	}
	if got := target.store.GetRecords(movedKey); len(got) != 2 {
		t.Fatalf("retry produced %d versions; want 2", len(got))
	}

	var secondKey int64
	foundSecond := false
	for key := int64(0); key < 10000; key++ {
		if key == movedKey {
			continue
		}
		diff, err := ring.DiffReplicasForKey(current, proposed, key, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Added) == 1 && diff.Added[0].ID == "node-3" {
			secondKey = key
			foundSecond = true
			break
		}
	}
	if !foundSecond {
		t.Fatal("no second moved key found")
	}

	second := store.Record{
		Value: "second-key",
		Clock: version.Clock{"node-1": 2},
	}
	if err := source.store.ApplyRecord(secondKey, second); err != nil {
		t.Fatal(err)
	}

	progress, err := source.preCopyPlannedKeys(ctx, proposed)
	if err != nil {
		t.Fatal(err)
	}
	if progress.PlannedKeys != 2 ||
		progress.CompletedKeys != 2 ||
		progress.RecordsSent != 3 {
		t.Fatalf("unexpected pre-copy progress: %+v", progress)
	}
	if got := target.store.GetRecords(secondKey); len(got) != 1 ||
		!containsRecord(got, second) {
		t.Fatalf("target is missing second key: %+v", got)
	}
}
