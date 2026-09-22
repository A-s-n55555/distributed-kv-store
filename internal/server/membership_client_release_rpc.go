package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReleaseJoinClients persists local release before opening the client gate.
func (s *GRPCServer) ReleaseJoinClients(
	ctx context.Context,
	request *kvpb.PrepareJoinPauseRequest,
) (*kvpb.MembershipStatusResponse, error) {
	if err := s.authorizeMembershipControl(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	expectedOld, err := membershipIdentityFromRequest(
		request.GetActiveEpoch(), request.GetActiveDigest(),
	)
	if err != nil {
		return nil, err
	}
	expectedCandidate, err := membershipIdentityFromRequest(
		request.GetCandidateEpoch(), request.GetCandidateDigest(),
	)
	if err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	active := s.activeMembership
	path := s.joinPausePath
	s.writeMu.Unlock()

	if active.Identity() != expectedCandidate {
		return nil, status.Error(
			codes.FailedPrecondition,
			"requested candidate is not active",
		)
	}

	previous, err := membership.LoadActivatedJoin(path, active)
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition, "load activated join: %v", err,
		)
	}
	if _, err := membership.ValidateJoinIntent(
		previous,
		expectedOld,
		ring.Node{
			ID:      request.GetJoiningNodeId(),
			Address: request.GetJoiningNodeAddress(),
		},
		expectedCandidate,
	); err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition, "client-release intent rejected: %v", err,
		)
	}

	released, err := membership.LoadJoinClientsReleased(path, active)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "inspect client release: %v", err,
		)
	}
	if !released {
		if err := s.requireJoinClientReleaseDecision(
			ctx, previous, active, request,
		); err != nil {
			return nil, err
		}
	}

	err = func() error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		s.replicaApplyMu.Lock()
		defer s.replicaApplyMu.Unlock()

		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if s.activeMembership.Identity() != expectedCandidate ||
			s.joinPausePath != path ||
			s.pendingJoin == nil ||
			*s.pendingJoin != expectedCandidate {
			return status.Error(
				codes.FailedPrecondition,
				"local membership changed during client release",
			)
		}

		ready, err := membership.LoadJoinReplicaReady(path, active)
		if err != nil {
			return status.Errorf(
				codes.Internal, "inspect replica readiness: %v", err,
			)
		}
		if !ready || s.replicasPaused {
			return status.Error(
				codes.FailedPrecondition,
				"replicas must be enabled before clients",
			)
		}

		released, err := membership.LoadJoinClientsReleased(path, active)
		if err != nil {
			return status.Errorf(
				codes.Internal, "recheck client release: %v", err,
			)
		}
		if !s.writesPaused && !released {
			return status.Error(
				codes.FailedPrecondition,
				"clients are open without a release marker",
			)
		}

		if err := membership.RecordJoinClientsReleased(path, active); err != nil {
			return status.Errorf(
				codes.Internal, "persist client release: %v", err,
			)
		}

		s.writesPaused = false
		return nil
	}()
	if err != nil {
		return nil, err
	}

	return s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
}
