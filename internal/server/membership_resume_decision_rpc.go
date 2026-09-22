package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetJoinResumeDecision reports the saved decision without changing gates.
func (s *GRPCServer) GetJoinResumeDecision(
	ctx context.Context,
	request *kvpb.PrepareJoinPauseRequest,
) (*kvpb.JoinResumeDecisionResponse, error) {
	if err := s.authorizeMembershipControl(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
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
	defer s.writeMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	active := s.activeMembership
	if active.Identity() != expectedCandidate {
		return nil, status.Error(
			codes.FailedPrecondition,
			"requested candidate is not active",
		)
	}

	previous, err := membership.LoadActivatedJoin(s.joinPausePath, active)
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"load activated join: %v", err,
		)
	}
	if err := s.requireJoinCoordinator(previous); err != nil {
		return nil, err
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
			codes.FailedPrecondition,
			"resume intent rejected: %v", err,
		)
	}

	recorded, err := membership.LoadJoinResumeDecision(
		s.joinPausePath, active,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "read resume decision: %v", err,
		)
	}

	releaseDecided, err := membership.LoadJoinClientReleaseDecision(
		s.joinPausePath, active,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "read client-release decision: %v", err,
		)
	}
	if releaseDecided && !recorded {
		return nil, status.Error(
			codes.Internal,
			"client-release decision exists without resume authorization",
		)
	}

	return &kvpb.JoinResumeDecisionResponse{
		CoordinatorNodeId:     s.nodeID,
		ActiveEpoch:           expectedOld.Epoch,
		ActiveDigest:          append([]byte(nil), expectedOld.Digest[:]...),
		CandidateEpoch:        expectedCandidate.Epoch,
		CandidateDigest:       append([]byte(nil), expectedCandidate.Digest[:]...),
		ResumeDecided:         recorded,
		ClientsReleaseDecided: releaseDecided,
	}, nil
}
