package server

import (
	"context"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) applyRecordToReplica(
	ctx context.Context,
	node ring.Node,
	key int64,
	record store.Record,
) error {
	request := &kvpb.ReplicaRecordRequest{
		Key:    key,
		Record: recordToProto(record),
	}

	if node.ID == s.nodeID {
		_, err := s.ApplyReplicaRecord(ctx, request)
		return err
	}

	connection, err := grpc.NewClient(
		node.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return status.Errorf(
			codes.Unavailable,
			"connect to replica %s failed: %v",
			node.ID,
			err,
		)
	}
	defer connection.Close()

	replicaContext, cancel := context.WithTimeout(
		ctx,
		2*time.Second,
	)
	defer cancel()

	client := kvpb.NewKeyValueStoreClient(connection)

	_, err = client.ApplyReplicaRecord(replicaContext, request)
	return err
}

func (s *GRPCServer) readRecordsFromReplica(
	ctx context.Context,
	node ring.Node,
	key int64,
) ([]store.Record, error) {
	request := &kvpb.GetRequest{Key: key}

	var response *kvpb.ReplicaRecordReadResponse
	var err error

	if node.ID == s.nodeID {
		response, err = s.ReadReplicaRecord(ctx, request)
	} else {
		connection, connectError := grpc.NewClient(
			node.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if connectError != nil {
			return nil, status.Errorf(
				codes.Unavailable,
				"connect to replica %s failed: %v",
				node.ID,
				connectError,
			)
		}
		defer connection.Close()

		replicaContext, cancel := context.WithTimeout(
			ctx,
			2*time.Second,
		)
		defer cancel()

		client := kvpb.NewKeyValueStoreClient(connection)

		response, err = client.ReadReplicaRecord(
			replicaContext,
			request,
		)
	}

	if err != nil {
		return nil, err
	}

	return recordsFromReplicaResponse(response)
}

// Temporary compatibility wrapper for single-record callers.
func (s *GRPCServer) readRecordFromReplica(
	ctx context.Context,
	node ring.Node,
	key int64,
) (store.Record, bool, error) {
	records, err := s.readRecordsFromReplica(ctx, node, key)
	if err != nil {
		return store.Record{}, false, err
	}

	switch len(records) {
	case 0:
		return store.Record{}, false, nil

	case 1:
		return records[0], true, nil

	default:
		return store.Record{}, false, status.Error(
			codes.Aborted,
			"replica contains multiple concurrent versions",
		)
	}
}

func recordsFromReplicaResponse(
	response *kvpb.ReplicaRecordReadResponse,
) ([]store.Record, error) {
	if response == nil {
		return nil, status.Error(
			codes.Internal,
			"replica returned a nil response",
		)
	}

	if !response.GetExists() {
		if len(response.GetRecords()) != 0 ||
			response.GetRecord() != nil {
			return nil, status.Error(
				codes.Internal,
				"replica reported missing data with record contents",
			)
		}

		return nil, nil
	}

	wireRecords := response.GetRecords()

	// Support the previous single-record response format.
	if len(wireRecords) == 0 && response.GetRecord() != nil {
		wireRecords = []*kvpb.VersionedRecord{
			response.GetRecord(),
		}
	}

	if len(wireRecords) == 0 {
		return nil, status.Error(
			codes.Internal,
			"replica reported existing data without records",
		)
	}

	records := make([]store.Record, 0, len(wireRecords))

	for _, wireRecord := range wireRecords {
		if wireRecord == nil {
			return nil, status.Error(
				codes.Internal,
				"replica returned a nil record",
			)
		}

		if wireRecord.GetDeleted() &&
			wireRecord.GetValue() != "" {
			return nil, status.Error(
				codes.Internal,
				"replica returned a tombstone with a value",
			)
		}

		for nodeID, counter := range wireRecord.GetClock() {
			if nodeID == "" || counter == 0 {
				return nil, status.Error(
					codes.Internal,
					"replica returned an invalid vector clock",
				)
			}
		}

		// Empty clocks remain allowed for legacy stored records.
		records = append(records, recordFromProto(wireRecord))
	}

	return records, nil
}
