package server

import (
	"bytes"
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AbortJoinPause removes this node's durable pause for one exact join.
func (s *GRPCServer) AbortJoinPause(
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

	if err := s.AbortForJoin(candidate); err != nil {
		return nil, err
	}

	response, err := s.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		return nil, err
	}

	// if statusErr != nil {
	// 	if resumeErr != nil {
	// 		return nil, resumeErr
	// 	}
	// 	return nil, statusErr
	// }

	if response.GetActiveEpoch() != expectedActive.Epoch ||
		!bytes.Equal(response.GetActiveDigest(), expectedActive.Digest[:]) ||
		response.GetWritesPaused() ||
		response.GetPendingEpoch() != 0 ||
		len(response.GetPendingDigest()) != 0 {
		// if resumeErr != nil {
		// 	return nil, resumeErr
		// }
		return nil, status.Error(
			codes.FailedPrecondition,
			"node did not confirm the aborted join",
		)
	}

	return response, nil
}
