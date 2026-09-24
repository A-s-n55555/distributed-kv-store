package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CoordinateJoinResume completes replica readiness and client release.
func (s *GRPCServer) CoordinateJoinResume(
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
			"candidate membership is not active on coordinator",
		)
	}

	previous, err := membership.LoadActivatedJoin(path, active)
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"recover original membership: %v", err,
		)
	}

	candidate, err := membership.ValidateJoinIntent(
		previous,
		expectedOld,
		ring.Node{
			ID:      request.GetJoiningNodeId(),
			Address: request.GetJoiningNodeAddress(),
		},
		expectedCandidate,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"resume intent rejected: %v", err,
		)
	}
	if err := s.requireJoinCoordinator(previous); err != nil {
		return nil, err
	}

	if err := s.resumeActivatedJoin(ctx, candidate); err != nil {
		return nil, err
	}
	return s.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
}
