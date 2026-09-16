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

func (s *GRPCServer) readRecordFromReplica(
	ctx context.Context,
	node ring.Node,
	key int64,
) (store.Record, bool, error) {
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
			return store.Record{}, false, status.Errorf(
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
		return store.Record{}, false, err
	}

	if !response.GetExists() {
		return store.Record{}, false, nil
	}

	if response.GetRecord() == nil {
		return store.Record{}, false, status.Error(
			codes.Internal,
			"replica reported an existing record without its contents",
		)
	}

	return recordFromProto(response.GetRecord()), true, nil
}
