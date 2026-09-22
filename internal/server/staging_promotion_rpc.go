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

// PromoteJoin records promotion while keeping both request gates closed.
func (s *StagingService) PromoteJoin(
	ctx context.Context,
	request *kvpb.PrepareJoinPauseRequest,
) (*kvpb.PromotionStatusResponse, error) {
	if err := s.target.authorizeMembershipControl(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.promotionPausePath == "" {
		return nil, status.Error(
			codes.FailedPrecondition,
			"promotion recovery is not configured",
		)
	}

	expectedCurrent, err := membershipIdentityFromRequest(
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

	candidate, err := membership.ValidateJoinIntent(
		s.joinCurrent,
		expectedCurrent,
		ring.Node{
			ID:      request.GetJoiningNodeId(),
			Address: request.GetJoiningNodeAddress(),
		},
		expectedCandidate,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"invalid promotion intent: %v", err,
		)
	}
	if candidate.Identity() != s.joinCandidate.Identity() {
		return nil, status.Error(
			codes.FailedPrecondition,
			"promotion differs from configured staging join",
		)
	}

	previous, saved, err := membership.LoadStagingPromotion(
		s.promotionPausePath, s.target.nodeID,
	)
	switch {
	case err == nil:
		// A durable decision can be retried without contacting old members.
		if previous.Identity() != expectedCurrent ||
			saved.Identity() != expectedCandidate {
			return nil, status.Error(
				codes.FailedPrecondition,
				"another promotion is already recorded",
			)
		}

	case errors.Is(err, os.ErrNotExist):
		if err := s.verifyPromotionReady(
			ctx, expectedCurrent, expectedCandidate,
		); err != nil {
			return nil, err
		}

	default:
		return nil, status.Errorf(
			codes.Internal,
			"inspect saved promotion: %v", err,
		)
	}

	s.target.writeMu.Lock()
	defer s.target.writeMu.Unlock()
	s.target.replicaApplyMu.Lock()
	defer s.target.replicaApplyMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	// Drain in-flight replica applies, then block new ones before persisting.
	s.target.writesPaused = true
	s.target.replicasPaused = true

	if err := membership.RecordStagingPromotion(
		s.promotionPausePath,
		s.target.nodeID,
		s.joinCurrent,
		s.joinCandidate,
	); err != nil {
		// Keep the gates closed: persistence may have partially completed.
		return nil, status.Errorf(
			codes.Internal,
			"record staging promotion: %v", err,
		)
	}

	if err := s.installPromotedMembershipLocked(); err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"install promoted membership: %v", err,
		)
	}

	return &kvpb.PromotionStatusResponse{
		NodeId:            s.target.nodeID,
		CandidateEpoch:    expectedCandidate.Epoch,
		CandidateDigest:   append([]byte(nil), expectedCandidate.Digest[:]...),
		PromotionRecorded: true,
		RequestsPaused:    true,
	}, nil
}
