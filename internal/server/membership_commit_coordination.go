package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func confirmsJoinCommit(
	response *kvpb.MembershipStatusResponse,
	nodeID string,
	active, candidate membership.Identity,
) bool {
	return confirmsJoinPause(response, nodeID, active, candidate) &&
		response.GetJoinCommitted()
}

// commitOldMembersForJoin completes catch-up, then durably commits each old
// member's pause. On failure, retry this function with the same candidate;
// never abort automatically after commitment may have begun.
func (s *GRPCServer) commitOldMembersForJoin(
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

	activeID := current.Identity()
	candidateID := candidate.Identity()

	local, err := s.GetMembershipStatus(
		ctx, &kvpb.MembershipStatusRequest{},
	)
	if err != nil {
		return err
	}

	// A retry after local commitment must finish the remaining commits.
	// Otherwise, run the paused catch-up and verification before committing.
	if !confirmsJoinCommit(local, s.nodeID, activeID, candidateID) {
		if _, err := s.CatchUpJoin(ctx, candidate); err != nil {
			return err
		}
	}

	// Check every old member before recording the first new commitment.
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
				codes.Unavailable, "check pause on %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinPause(response, node.ID, activeID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s is not paused for this join", node.ID,
			)
		}
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	// Commit locally first so this coordinator's durable marker prevents
	// its own abort path from reopening writes during a retry.
	if err := s.CommitForJoin(candidate); err != nil {
		return err
	}

	for _, node := range current.Nodes() {
		if node.ID == s.nodeID {
			continue
		}

		if _, err := s.commitJoinPeer(ctx, node, current, candidate); err != nil {
			// The RPC may have saved the commit before its reply was lost.
			observed, readErr := s.fetchMembershipStatus(ctx, node)
			if readErr != nil ||
				!confirmsJoinCommit(observed, node.ID, activeID, candidateID) {
				return status.Errorf(
					codes.Unavailable,
					"commit of %s is uncertain: RPC: %v; status read: %v",
					node.ID, err, readErr,
				)
			}
		}
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
				codes.Unavailable, "confirm commit on %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinCommit(response, node.ID, activeID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s did not confirm the committed join", node.ID,
			)
		}
	}

	return nil
}
