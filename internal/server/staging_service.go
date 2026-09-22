package server

import (
	"context"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

// StagingService exposes only record transfer RPCs before a node joins.
type StagingService struct {
	kvpb.UnimplementedKeyValueStoreServer
	target             *GRPCServer
	promotionPausePath string // configured before serving RPCs

	// Configured once, before serving RPCs.
	joinCurrent   membership.Configuration
	joinCandidate membership.Configuration
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

// ConfigureJoin binds this staging service to one validated transition.
// Call it before starting the gRPC server.
func (s *StagingService) ConfigureJoin(
	current, candidate membership.Configuration,
) error {
	if s.joinCurrent.Identity().Epoch != 0 {
		return fmt.Errorf("staging join is already configured")
	}

	joining, err := membership.ValidateJoinCandidate(current, candidate)
	if err != nil {
		return fmt.Errorf("invalid staging join: %w", err)
	}
	if joining.ID != s.target.nodeID {
		return fmt.Errorf(
			"staging node %s is not joining node %s",
			s.target.nodeID, joining.ID,
		)
	}
	if s.target.replicationFactor != candidate.ReplicationFactor() {
		return fmt.Errorf("staging replication factor differs from candidate")
	}

	s.joinCurrent = current
	s.joinCandidate = candidate
	return nil
}
func (s *StagingService) EnableJoinReplicas(
	ctx context.Context,
	request *kvpb.PrepareJoinPauseRequest,
) (*kvpb.MembershipStatusResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.EnableJoinReplicas(ctx, request)
}
func (s *StagingService) ReleaseJoinClients(
	ctx context.Context,
	request *kvpb.PrepareJoinPauseRequest,
) (*kvpb.MembershipStatusResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.ReleaseJoinClients(ctx, request)
}