package server

import (
	"context"
	"errors"
	"os"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetJoinDecision reads the coordinator's durable decision.
// It never creates a decision or changes either write gate.
func (s *GRPCServer) GetJoinDecision(
	ctx context.Context,
	request *kvpb.PrepareJoinPauseRequest,
) (*kvpb.JoinDecisionResponse, error) {
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
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	current := s.activeMembership

	if s.pendingJoin != nil &&
		*s.pendingJoin == current.Identity() {
		if !s.writesPaused || !s.replicasPaused {
			return nil, status.Error(
				codes.FailedPrecondition,
				"activated join is not paused",
			)
		}

		previous, err := membership.LoadActivatedJoin(
			s.joinPausePath, current,
		)
		if err != nil {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"recover activated join decision: %v", err,
			)
		}
		current = previous
	}

	if err := s.requireJoinCoordinator(current); err != nil {
		return nil, err
	}
	if s.joinPausePath == "" {
		return nil, status.Error(
			codes.FailedPrecondition, "join pause path is not configured",
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
			codes.FailedPrecondition, "join intent rejected: %v", err,
		)
	}

	saved, err := membership.LoadJoinCommit(s.joinPausePath, current)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(
			codes.Internal, "read join commitment: %v", err,
		)
	}
	committed := err == nil && saved.Identity() == candidate.Identity()

	aborted, err := membership.JoinWasAborted(
		s.joinPausePath, current, candidate,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "read join abort decision: %v", err,
		)
	}
	if committed && aborted {
		return nil, status.Error(
			codes.Internal, "candidate has conflicting saved decisions",
		)
	}

	decision := kvpb.JoinDecision_JOIN_DECISION_UNDECIDED
	if committed {
		decision = kvpb.JoinDecision_JOIN_DECISION_COMMIT
	} else if aborted {
		decision = kvpb.JoinDecision_JOIN_DECISION_ABORT
	}

	return &kvpb.JoinDecisionResponse{
		CoordinatorNodeId: s.nodeID,
		ActiveEpoch:       expectedActive.Epoch,
		ActiveDigest:      append([]byte(nil), expectedActive.Digest[:]...),
		CandidateEpoch:    expectedCandidate.Epoch,
		CandidateDigest:   append([]byte(nil), expectedCandidate.Digest[:]...),
		Decision:          decision,
	}, nil
}
