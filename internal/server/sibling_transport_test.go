package server

import (
	"testing"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicaResponsePreservesSiblings(t *testing.T) {
	response := &kvpb.ReplicaRecordReadResponse{
		Exists: true,
		Records: []*kvpb.VersionedRecord{
			{
				Value: "first",
				Clock: map[string]uint64{"node-1": 1},
			},
			{
				Value: "second",
				Clock: map[string]uint64{"node-2": 1},
			},
		},
	}

	records, err := recordsFromReplicaResponse(response)
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 2 ||
		records[0].Value != "first" ||
		records[1].Value != "second" {
		t.Fatalf("incorrect siblings: %+v", records)
	}

	response.Records[0].Clock["node-1"] = 999

	if records[0].Clock["node-1"] != 1 {
		t.Fatal("converted records shared the response clock")
	}
}

func TestReplicaResponseSupportsLegacyField(t *testing.T) {
	records, err := recordsFromReplicaResponse(
		&kvpb.ReplicaRecordReadResponse{
			Exists: true,
			Record: &kvpb.VersionedRecord{
				Value: "legacy",
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 || records[0].Value != "legacy" {
		t.Fatalf("incorrect legacy conversion: %+v", records)
	}
}

func TestReplicaResponseRejectsMalformedData(t *testing.T) {
	tests := []*kvpb.ReplicaRecordReadResponse{
		nil,
		{Exists: true},
		{
			Exists:  true,
			Records: []*kvpb.VersionedRecord{nil},
		},
		{
			Exists: false,
			Record: &kvpb.VersionedRecord{Value: "unexpected"},
		},
	}

	for i, response := range tests {
		_, err := recordsFromReplicaResponse(response)

		if status.Code(err) != codes.Internal {
			t.Fatalf(
				"case %d: error=%v; want Internal",
				i,
				err,
			)
		}
	}
}
