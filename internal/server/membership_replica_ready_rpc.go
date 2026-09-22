package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) EnableJoinReplicas(
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
			codes.FailedPrecondition, "replica-ready intent rejected: %v", err,
		)
	}

	ready, err := membership.LoadJoinReplicaReady(path, active)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "inspect replica readiness: %v", err,
		)
	}
	if !ready {
		if err := s.requireJoinResumeDecision(
			ctx, previous, active, request,
		); err != nil {
			return nil, err
		}
	}

	// Release the locks before GetMembershipStatus acquires them again.
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
			!s.writesPaused ||
			s.pendingJoin == nil ||
			*s.pendingJoin != expectedCandidate {
			return status.Error(
				codes.FailedPrecondition,
				"node is not paused for this activated join",
			)
		}

		if err := membership.RecordJoinReplicaReady(path, active); err != nil {
			return status.Errorf(
				codes.Internal, "persist replica readiness: %v", err,
			)
		}

		s.replicasPaused = false
		return nil
	}()
	if err != nil {
		return nil, err
	}

	return s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
}
