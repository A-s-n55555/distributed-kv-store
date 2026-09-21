package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CommitForJoin saves the decision while both local gates remain closed.
func (s *GRPCServer) CommitForJoin(candidate membership.Configuration) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	current := s.activeMembership
	if current.Identity().Epoch == 0 || s.ring == nil {
		return status.Error(codes.FailedPrecondition, "active membership is unavailable")
	}
	actual, err := membership.NewConfiguration(
		current.Identity().Epoch, s.ring, s.replicationFactor,
	)
	if err != nil || actual.Identity() != current.Identity() {
		return status.Error(codes.FailedPrecondition, "server ring differs from active membership")
	}
	if s.pendingJoin == nil ||
		*s.pendingJoin != candidate.Identity() ||
		!s.writesPaused || !s.replicasPaused {
		return status.Error(codes.FailedPrecondition, "node is not paused for this join")
	}
	if err := membership.RecordJoinCommit(
		s.joinPausePath, current, candidate,
	); err != nil {
		return status.Errorf(codes.FailedPrecondition, "record join commitment: %v", err)
	}
	return nil
}

// CommitJoinPause commits one exact candidate without opening either gate.
func (s *GRPCServer) CommitJoinPause(
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

	s.writeMu.Lock()
	current := s.activeMembership
	s.writeMu.Unlock()

	candidate, err := membership.ValidateJoinIntent(
		current, expectedActive,
		ring.Node{
			ID:      request.GetJoiningNodeId(),
			Address: request.GetJoiningNodeAddress(),
		},
		expectedCandidate,
	)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "join intent rejected: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if err := s.requireJoinDecision(
		ctx, current, candidate, kvpb.JoinDecision_JOIN_DECISION_COMMIT,
	); err != nil {
		return nil, err
	}

	if err := s.CommitForJoin(candidate); err != nil {
		return nil, err
	}
	return s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
}
