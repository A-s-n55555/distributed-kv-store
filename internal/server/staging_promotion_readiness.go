package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// verifyPromotionReady checks the requested transition and requires every
// old member to report the candidate as active, committed, and still paused.
//
// This is a read-only helper. The future promotion RPC must authorize the
// caller before calling it.
func (s *StagingService) verifyPromotionReady(
	ctx context.Context,
	expectedCurrent, expectedCandidate membership.Identity,
) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	current := s.joinCurrent
	candidate := s.joinCandidate

	if current.Identity().Epoch == 0 ||
		candidate.Identity().Epoch == 0 {
		return status.Error(
			codes.FailedPrecondition,
			"staging join is not configured",
		)
	}
	if current.Identity() != expectedCurrent ||
		candidate.Identity() != expectedCandidate {
		return status.Error(
			codes.FailedPrecondition,
			"promotion request differs from configured staging join",
		)
	}

	for _, node := range current.Nodes() {
		response, err := s.target.fetchMembershipStatus(ctx, node)
		if err != nil {
			return status.Errorf(
				codes.Unavailable,
				"check old member %s before promotion: %v",
				node.ID, err,
			)
		}

		if !confirmsJoinActivation(
			response, node.ID, expectedCandidate,
		) {
			return status.Errorf(
				codes.FailedPrecondition,
				"old member %s has not activated this join while paused",
				node.ID,
			)
		}
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
