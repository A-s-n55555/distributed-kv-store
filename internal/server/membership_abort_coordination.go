package server

import (
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func confirmsJoinAbort(
	response *kvpb.MembershipStatusResponse,
	nodeID string,
	active membership.Identity,
) bool {
	return response != nil &&
		response.GetNodeId() == nodeID &&
		response.GetActiveEpoch() == active.Epoch &&
		bytes.Equal(response.GetActiveDigest(), active.Digest[:]) &&
		!response.GetWritesPaused() &&
		response.GetPendingEpoch() == 0 &&
		len(response.GetPendingDigest()) == 0
}

// recordJoinAbortDecision saves the abort choice without opening either gate.
// The coordinator can retry remote aborts after a failure or restart.
func (s *GRPCServer) recordJoinAbortDecision(
	current, candidate membership.Configuration,
) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if s.activeMembership.Identity() != current.Identity() {
		return status.Error(codes.FailedPrecondition, "active membership changed")
	}
	if s.joinPausePath == "" {
		return status.Error(codes.FailedPrecondition, "join pause path is not configured")
	}

	pausedForCandidate := s.writesPaused && s.replicasPaused &&
		s.pendingJoin != nil && *s.pendingJoin == candidate.Identity()
	if !pausedForCandidate &&
		(s.writesPaused || s.replicasPaused || s.pendingJoin != nil) {
		return status.Error(codes.FailedPrecondition, "node is paused for another operation")
	}

	if err := s.requireJoinCoordinator(current); err != nil {
		return err
	}

	if _, err := membership.LoadJoinCommit(
		s.joinPausePath, current,
	); err == nil {
		return status.Error(codes.FailedPrecondition, "committed join cannot be aborted")
	} else if !errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.Internal, "inspect join commitment: %v", err)
	}

	if err := membership.RecordJoinAbort(
		s.joinPausePath, current, candidate,
	); err != nil {
		return status.Errorf(codes.Internal, "persist join abort decision: %v", err)
	}
	return nil
}

// abortOldMembersForJoin tries every remote old member, then resumes local
// writes only when all remote members have confirmed the old active state.
func (s *GRPCServer) abortOldMembersForJoin(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	s.writeMu.Lock()
	current := s.activeMembership
	s.writeMu.Unlock()

	if err := s.requireJoinCoordinator(current); err != nil {
		return err
	}

	if _, err := membership.ValidateJoinCandidate(current, candidate); err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid join candidate: %v", err)
	}

	actual, err := membership.NewConfiguration(
		current.Identity().Epoch, s.ring, s.replicationFactor,
	)
	if err != nil || actual.Identity() != current.Identity() {
		return status.Error(codes.FailedPrecondition, "server ring differs from active membership")
	}

	if err := s.recordJoinAbortDecision(current, candidate); err != nil {
		return err
	}

	activeID := current.Identity()
	var firstRemoteError error

	for _, node := range current.Nodes() {
		if node.ID == s.nodeID {
			continue
		}

		if _, err := s.abortJoinPeer(ctx, node, current, candidate); err != nil {
			if firstRemoteError == nil {
				firstRemoteError = status.Errorf(
					codes.Unavailable, "abort of %s is uncertain: %v",
					node.ID, err,
				)
			}
		}
	}

	// Leave this node paused when any remote state remains uncertain.
	if firstRemoteError != nil {
		return firstRemoteError
	}

	if err := s.AbortForJoin(candidate); err != nil {
		return err
	}

	for _, node := range current.Nodes() {
		var response *kvpb.MembershipStatusResponse
		if node.ID == s.nodeID {
			response, err = s.GetMembershipStatus(
				ctx, &kvpb.MembershipStatusRequest{},
			)
		} else {
			response, err = s.fetchMembershipStatus(ctx, node)
		}
		if err != nil {
			return status.Errorf(
				codes.Unavailable, "confirm abort on %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinAbort(response, node.ID, activeID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s did not confirm the aborted join", node.ID,
			)
		}
	}

	return nil
}
