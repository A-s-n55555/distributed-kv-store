package server

import (
	"bytes"
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkJoinReadiness checks every old member before starting a join attempt.
func (s *GRPCServer) checkJoinReadiness(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	s.writeMu.Lock()
	current := s.activeMembership
	s.writeMu.Unlock()

	activeID := current.Identity()
	if activeID.Epoch == 0 || s.ring == nil {
		return status.Error(codes.FailedPrecondition, "active membership is unavailable")
	}

	actual, err := membership.NewConfiguration(
		activeID.Epoch, s.ring, s.replicationFactor,
	)
	if err != nil || actual.Identity() != activeID {
		return status.Error(codes.FailedPrecondition, "server ring differs from active membership")
	}
	if _, err := membership.ValidateJoinCandidate(current, candidate); err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid join candidate: %v", err)
	}

	candidateID := candidate.Identity()
	foundLocal := false

	for _, node := range current.Nodes() {
		var response *kvpb.MembershipStatusResponse
		if node.ID == s.nodeID {
			foundLocal = true
			response, err = s.GetMembershipStatus(
				ctx, &kvpb.MembershipStatusRequest{},
			)
		} else {
			response, err = s.fetchMembershipStatus(ctx, node)
		}
		if err != nil {
			return status.Errorf(
				codes.Unavailable, "membership status from %s: %v", node.ID, err,
			)
		}
		if response.GetNodeId() != node.ID ||
			response.GetActiveEpoch() != activeID.Epoch ||
			!bytes.Equal(response.GetActiveDigest(), activeID.Digest[:]) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s has a different active membership", node.ID,
			)
		}

		if response.GetWritesPaused() {
			if response.GetPendingEpoch() != candidateID.Epoch ||
				!bytes.Equal(response.GetPendingDigest(), candidateID.Digest[:]) {
				return status.Errorf(
					codes.FailedPrecondition,
					"node %s is paused for another operation", node.ID,
				)
			}
		} else if response.GetPendingEpoch() != 0 ||
			len(response.GetPendingDigest()) != 0 {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s has inconsistent pending membership", node.ID,
			)
		}
	}

	if !foundLocal {
		return status.Error(codes.FailedPrecondition, "local node is absent from active membership")
	}
	return nil
}
