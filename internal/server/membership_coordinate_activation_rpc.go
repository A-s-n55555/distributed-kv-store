package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CoordinateJoinActivation activates all old members for one committed join.
func (s *GRPCServer) CoordinateJoinActivation(
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
	current := s.activeMembership
	if current.Identity() == expectedCandidate {
		// Node-1 may have activated before an interruption.
		current, err = membership.LoadActivatedJoin(
			s.joinPausePath, current,
		)
	}
	s.writeMu.Unlock()
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"recover original membership: %v", err,
		)
	}

	candidate, err := membership.ValidateJoinIntent(
		current,
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
			"activation intent rejected: %v", err,
		)
	}
	if err := s.requireJoinCoordinator(current); err != nil {
		return nil, err
	}

	if err := s.activateOldMembersForJoin(ctx, candidate); err != nil {
		return nil, err
	}
	return s.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
}
