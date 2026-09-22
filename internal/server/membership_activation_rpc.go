package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ActivateJoin validates authorization and the exact join before activation.
func (s *GRPCServer) ActivateJoin(
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

	expectedActive, err := membershipIdentityFromRequest(
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

	// Recover the original membership for a repeated activation request.
	s.writeMu.Lock()
	current := s.activeMembership
	alreadyActive := current.Identity() == expectedCandidate
	if alreadyActive {
		current, err = membership.LoadActivatedJoin(
			s.joinPausePath, current,
		)
	}
	s.writeMu.Unlock()
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"recover activated join: %v", err,
		)
	}

	candidate, err := membership.ValidateJoinIntent(
		current,
		expectedActive,
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

	if !alreadyActive {
		if err := s.requireJoinDecision(
			ctx, current, candidate,
			kvpb.JoinDecision_JOIN_DECISION_COMMIT,
		); err != nil {
			return nil, err
		}
	}
	// For a retry, LoadActivatedJoin verified the durable local decision.
	// ActivateForJoin rechecks the markers and closed gates under lock.

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := s.ActivateForJoin(candidate); err != nil {
		return nil, err
	}
	return s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
}
