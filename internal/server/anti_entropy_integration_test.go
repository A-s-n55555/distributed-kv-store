package server

import (
	"context"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func TestAntiEntropyRepairsMissingSiblingsAndTombstone(t *testing.T) {
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener1.Close()

	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener2.Close()

	node1 := ring.Node{
		ID:      "node-1",
		Address: listener1.Addr().String(),
	}
	node2 := ring.Node{
		ID:      "node-2",
		Address: listener2.Addr().String(),
	}

	clusterRing := ring.New(32)
	clusterRing.AddNode(node1)
	clusterRing.AddNode(node2)

	service1 := New(
		newAntiEntropyTestStore(t),
		clusterRing,
		node1.ID,
		2,
		1,
		2,
		nil,
	)

	service2 := New(
		newAntiEntropyTestStore(t),
		clusterRing,
		node2.ID,
		2,
		1,
		2,
		nil,
	)

	grpcServer1 := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(grpcServer1, service1)

	grpcServer2 := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(grpcServer2, service2)

	go func() {
		_ = grpcServer1.Serve(listener1)
	}()
	go func() {
		_ = grpcServer2.Serve(listener2)
	}()

	t.Cleanup(grpcServer1.Stop)
	t.Cleanup(grpcServer2.Stop)

	const key int64 = 9450

	// These clocks are concurrent, so both versions must remain stored.
	liveSibling := store.Record{
		Value: "live-value",
		Clock: version.Clock{"node-1": 1},
	}

	tombstoneSibling := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}

	if err := service1.store.ApplyRecord(key, liveSibling); err != nil {
		t.Fatal(err)
	}

	if err := service1.store.ApplyRecord(key, tombstoneSibling); err != nil {
		t.Fatal(err)
	}

	if records := service2.store.GetRecords(key); len(records) != 0 {
		t.Fatalf(
			"node-2 unexpectedly had %d versions before synchronization",
			len(records),
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	applied, err := service2.syncAntiEntropyFromPeer(ctx, node1)
	if err != nil {
		t.Fatal(err)
	}

	if applied != 2 {
		t.Fatalf("applied %d versions; want 2", applied)
	}

	got := service2.store.GetRecords(key)

	if len(got) != 2 {
		t.Fatalf("node-2 has %d versions; want 2", len(got))
	}

	if !containsRecord(got, liveSibling) {
		t.Fatalf("node-2 is missing live sibling: %+v", got)
	}

	if !containsRecord(got, tombstoneSibling) {
		t.Fatalf("node-2 is missing tombstone sibling: %+v", got)
	}

	// A matching peer must not transfer records again.
	applied, err = service2.syncAntiEntropyFromPeer(ctx, node1)
	if err != nil {
		t.Fatal(err)
	}

	if applied != 0 {
		t.Fatalf(
			"matching nodes applied %d versions; want 0",
			applied,
		)
	}
}

func newAntiEntropyTestStore(t *testing.T) *store.Map {
	t.Helper()

	journal, err := wal.Open(
		filepath.Join(t.TempDir(), "wal.log"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Errorf("WAL Close() error = %v", err)
		}
	})

	kvStore, err := store.NewMap(journal)
	if err != nil {
		t.Fatal(err)
	}

	return kvStore
}

func containsRecord(
	records []store.Record,
	target store.Record,
) bool {
	for _, record := range records {
		if reflect.DeepEqual(record, target) {
			return true
		}
	}

	return false
}
