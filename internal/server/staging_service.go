package server

import (
	"context"
	"fmt"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

// StagingService exposes only record transfer RPCs before a node joins.
type StagingService struct {
	kvpb.UnimplementedKeyValueStoreServer
	target *GRPCServer
}

func NewStagingService(target *GRPCServer) (*StagingService, error) {
	if target == nil || target.store == nil {
		return nil, fmt.Errorf("staging service requires a store")
	}
	return &StagingService{target: target}, nil
}

func (s *StagingService) ApplyReplicaRecord(
	ctx context.Context,
	request *kvpb.ReplicaRecordRequest,
) (*kvpb.ReplicaRecordResponse, error) {
	return s.target.ApplyReplicaRecord(ctx, request)
}

func (s *StagingService) ReadReplicaRecord(
	ctx context.Context,
	request *kvpb.GetRequest,
) (*kvpb.ReplicaRecordReadResponse, error) {
	return s.target.ReadReplicaRecord(ctx, request)
}
