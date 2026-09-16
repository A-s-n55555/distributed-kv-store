package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newRecordTestServer(t *testing.T) *GRPCServer {
	t.Helper()

	log, err := wal.Open(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	kvStore, err := store.NewMap(log)
	if err != nil {
		t.Fatal(err)
	}

	return &GRPCServer{
		store: kvStore,
	}
}

func TestReplicaRecordRoundTrip(t *testing.T) {
	s := newRecordTestServer(t)
	ctx := context.Background()

	request := &kvpb.ReplicaRecordRequest{
		Key: 1,
		Record: &kvpb.VersionedRecord{
			Value: "hello",
			Clock: map[string]uint64{"node-1": 1},
		},
	}

	if _, err := s.ApplyReplicaRecord(ctx, request); err != nil {
		t.Fatal(err)
	}

	// The store must not retain the request's mutable clock.
	request.Record.Clock["node-1"] = 999

	response, err := s.ReadReplicaRecord(
		ctx,
		&kvpb.GetRequest{Key: 1},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !response.GetExists() ||
		response.GetRecord().GetValue() != "hello" ||
		response.GetRecord().GetClock()["node-1"] != 1 {
		t.Fatalf("incorrect response: %v", response)
	}

	// The response must not expose the store's internal clock.
	response.Record.Clock["node-1"] = 888

	record, _ := s.store.GetRecord(1)
	if record.Clock["node-1"] != 1 {
		t.Fatal("response exposed the internal clock")
	}
}

func TestReplicaRecordReturnsTombstone(t *testing.T) {
	s := newRecordTestServer(t)
	ctx := context.Background()

	_, err := s.ApplyReplicaRecord(
		ctx,
		&kvpb.ReplicaRecordRequest{
			Key: 1,
			Record: &kvpb.VersionedRecord{
				Deleted: true,
				Clock:   map[string]uint64{"node-1": 2},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	response, err := s.ReadReplicaRecord(
		ctx,
		&kvpb.GetRequest{Key: 1},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !response.GetExists() ||
		!response.GetRecord().GetDeleted() ||
		response.GetRecord().GetClock()["node-1"] != 2 {
		t.Fatalf("incorrect tombstone response: %v", response)
	}

	missing, err := s.ReadReplicaRecord(
		ctx,
		&kvpb.GetRequest{Key: 99},
	)
	if err != nil {
		t.Fatal(err)
	}

	if missing.GetExists() || missing.GetRecord() != nil {
		t.Fatal("missing key was not distinguished from a tombstone")
	}
}

func TestReplicaRecordErrorCodes(t *testing.T) {
	s := newRecordTestServer(t)
	ctx := context.Background()

	_, err := s.ApplyReplicaRecord(
		ctx,
		&kvpb.ReplicaRecordRequest{
			Key: 1,
			Record: &kvpb.VersionedRecord{
				Value: "first",
				Clock: map[string]uint64{"node-1": 1},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		request *kvpb.ReplicaRecordRequest
		want    codes.Code
	}{
		{
			name:    "missing record",
			request: &kvpb.ReplicaRecordRequest{Key: 1},
			want:    codes.InvalidArgument,
		},
		{
			name: "missing clock",
			request: &kvpb.ReplicaRecordRequest{
				Key:    1,
				Record: &kvpb.VersionedRecord{Value: "hello"},
			},
			want: codes.InvalidArgument,
		},
		{
			name: "concurrent version",
			request: &kvpb.ReplicaRecordRequest{
				Key: 1,
				Record: &kvpb.VersionedRecord{
					Value: "second",
					Clock: map[string]uint64{"node-2": 1},
				},
			},
			want: codes.Aborted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := s.ApplyReplicaRecord(ctx, test.request)

			if status.Code(err) != test.want {
				t.Fatalf(
					"code = %v; want %v; error = %v",
					status.Code(err),
					test.want,
					err,
				)
			}
		})
	}
}
