package server

import (
	"context"
	"errors"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApplyReplicaRecord processes a record locally.
// It does not perform routing or further replication.
func (s *GRPCServer) ApplyReplicaRecord(
	_ context.Context,
	request *kvpb.ReplicaRecordRequest,
) (*kvpb.ReplicaRecordResponse, error) {
	incoming := request.GetRecord()

	if incoming == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"record is required",
		)
	}

	if len(incoming.GetClock()) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"vector clock is required",
		)
	}

	for nodeID, counter := range incoming.GetClock() {
		if nodeID == "" || counter == 0 {
			return nil, status.Error(
				codes.InvalidArgument,
				"clock entries require a node ID and positive counter",
			)
		}
	}

	if incoming.GetDeleted() && incoming.GetValue() != "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"a tombstone must have an empty value",
		)
	}

	err := s.store.ApplyRecord(
		request.GetKey(),
		recordFromProto(incoming),
	)

	if errors.Is(err, store.ErrRecordConflict) {
		return nil, status.Errorf(
			codes.Aborted,
			"record conflict: %v",
			err,
		)
	}

	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"apply record failed: %v",
			err,
		)
	}

	return &kvpb.ReplicaRecordResponse{}, nil
}

func (s *GRPCServer) ReadReplicaRecord(
	_ context.Context,
	request *kvpb.GetRequest,
) (*kvpb.ReplicaRecordReadResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	records := s.store.GetRecords(request.GetKey())

	if len(records) == 0 {
		return &kvpb.ReplicaRecordReadResponse{
			Exists: false,
		}, nil
	}

	wireRecords := make(
		[]*kvpb.VersionedRecord,
		0,
		len(records),
	)

	for _, record := range records {
		wireRecords = append(wireRecords, recordToProto(record))
	}

	response := &kvpb.ReplicaRecordReadResponse{
		Exists:  true,
		Records: wireRecords,
	}

	// Only provide a single-record view when it is unambiguous.
	if len(wireRecords) == 1 {
		response.Record = recordToProto(records[0])
	}

	return response, nil
}

func recordFromProto(record *kvpb.VersionedRecord) store.Record {
	return store.Record{
		Value: record.GetValue(),
		Clock: version.Clone(
			version.Clock(record.GetClock()),
		),
		Deleted: record.GetDeleted(),
	}
}

func recordToProto(record store.Record) *kvpb.VersionedRecord {
	return &kvpb.VersionedRecord{
		Value:   record.Value,
		Clock:   version.Clone(record.Clock),
		Deleted: record.Deleted,
	}
}
