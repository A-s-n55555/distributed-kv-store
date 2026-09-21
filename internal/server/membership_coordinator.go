package server

import (
	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) requireJoinCoordinator(
	current membership.Configuration,
) error {
	coordinator, err := membership.JoinCoordinator(current)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if coordinator.ID != s.nodeID {
		return status.Errorf(
			codes.FailedPrecondition,
			"join decision must be coordinated by %s",
			coordinator.ID,
		)
	}
	return nil
}
