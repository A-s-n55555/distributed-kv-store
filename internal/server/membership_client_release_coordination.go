package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// prepareJoinClientRelease verifies every candidate member before recording
// the coordinator's client-release decision.
func (s *GRPCServer) prepareJoinClientRelease(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	s.writeMu.Lock()
	active := s.activeMembership
	path := s.joinPausePath
	s.writeMu.Unlock()

	if active.Identity() != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator has not activated this candidate",
		)
	}
	previous, err := membership.LoadActivatedJoin(path, active)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if err := s.requireJoinCoordinator(previous); err != nil {
		return err
	}

	decided, err := membership.LoadJoinClientReleaseDecision(path, active)
	if err != nil {
		return status.Errorf(
			codes.Internal, "inspect client-release decision: %v", err,
		)
	}
	if decided {
		// Peers may already have released clients during an earlier attempt.
		return nil
	}

	for _, node := range candidate.Nodes() {
		var response *kvpb.MembershipStatusResponse
		if node.ID == s.nodeID {
			response, err = s.GetMembershipStatus(
				ctx, &kvpb.MembershipStatusRequest{},
			)
		} else {
			response, err = s.fetchMembershipStatus(ctx, node)
		}
		if err != nil {
			return status.Errorf(
				codes.Unavailable,
				"check %s before client release: %v", node.ID, err,
			)
		}
		if !confirmsJoinReplicaReady(
			response, node.ID, candidate.Identity(),
		) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s is not replica-ready with clients paused",
				node.ID,
			)
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.activeMembership.Identity() != candidate.Identity() ||
		s.joinPausePath != path {
		return status.Error(
			codes.FailedPrecondition,
			"local membership changed during release verification",
		)
	}

	decided, err = membership.LoadJoinClientReleaseDecision(path, candidate)
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}
	if decided {
		return nil
	}

	if !s.writesPaused || s.replicasPaused ||
		s.pendingJoin == nil ||
		*s.pendingJoin != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator is not replica-ready with clients paused",
		)
	}

	if err := membership.RecordJoinClientReleaseDecision(
		path, s.nodeID, candidate,
	); err != nil {
		return status.Errorf(
			codes.Internal, "record client-release decision: %v", err,
		)
	}
	return nil
}
