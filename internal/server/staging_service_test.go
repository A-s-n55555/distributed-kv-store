package server

import (
	"context"
	"testing"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStagingServiceTransfersRecordsButRejectsClientRPCs(t *testing.T) {
	staged, err := NewStagingService(newRecordTestServer(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := staged.Put(ctx, &kvpb.PutRequest{
		Key: 1, Value: "client-write",
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("staging Put: got %v, want Unimplemented", err)
	}
	if _, err := staged.Get(ctx, &kvpb.GetRequest{
		Key: 1,
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("staging Get: got %v, want Unimplemented", err)
	}

	_, err = staged.ApplyReplicaRecord(ctx, &kvpb.ReplicaRecordRequest{
		Key: 1,
		Record: &kvpb.VersionedRecord{
			Value: "staged",
			Clock: map[string]uint64{"node-1": 1},
		},
	})
	if err != nil {
		t.Fatalf("stage record: %v", err)
	}

	response, err := staged.ReadReplicaRecord(ctx, &kvpb.GetRequest{Key: 1})
	if err != nil {
		t.Fatalf("read staged record: %v", err)
	}
	if !response.GetExists() || len(response.GetRecords()) != 1 ||
		response.GetRecords()[0].GetValue() != "staged" {
		t.Fatalf("unexpected staged records: %v", response)
	}
}
