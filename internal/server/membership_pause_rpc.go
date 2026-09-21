package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func membershipIdentityFromRequest(
	epoch uint64,
	digest []byte,
) (membership.Identity, error) {
	var identity membership.Identity
	if epoch == 0 || len(digest) != len(identity.Digest) {
		return identity, status.Error(codes.InvalidArgument, "invalid membership identity")
	}
	identity.Epoch = epoch
	copy(identity.Digest[:], digest)
	return identity, nil
}

// PrepareJoinPause durably pauses this node for one exact join candidate.
func (s *GRPCServer) PrepareJoinPause(
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
		current,
		expectedActive,
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
	if err := s.PauseForJoin(candidate); err != nil {
		return nil, err
	}

	return s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
}
