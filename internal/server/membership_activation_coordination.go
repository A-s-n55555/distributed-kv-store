package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Activated members report the candidate as both active and pending,
// with their commit recorded and request gates still closed.
func confirmsJoinActivation(
	response *kvpb.MembershipStatusResponse,
	nodeID string,
	candidate membership.Identity,
) bool {
	return confirmsJoinCommit(response, nodeID, candidate, candidate) &&
		!response.GetReplicasReady()
}

// activateOldMembersForJoin runs after all old members have committed.
// On failure, retry this method with the same candidate.
func (s *GRPCServer) activateOldMembersForJoin(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	s.writeMu.Lock()
	current := s.activeMembership
	var recoveryErr error
	if current.Identity() == candidate.Identity() {
		current, recoveryErr = membership.LoadActivatedJoin(
			s.joinPausePath, current,
		)
	}
	s.writeMu.Unlock()

	if recoveryErr != nil {
		return status.Errorf(
			codes.FailedPrecondition,
			"recover original membership: %v", recoveryErr,
		)
	}
	if _, err := membership.ValidateJoinCandidate(
		current, candidate,
	); err != nil {
		return status.Errorf(
			codes.FailedPrecondition,
			"invalid activation candidate: %v", err,
		)
	}
	if err := s.requireJoinCoordinator(current); err != nil {
		return err
	}

	activeID := current.Identity()
	candidateID := candidate.Identity()

	readStatus := func(
		node ring.Node,
	) (*kvpb.MembershipStatusResponse, error) {
		if node.ID == s.nodeID {
			return s.GetMembershipStatus(
				ctx, &kvpb.MembershipStatusRequest{},
			)
		}
		return s.fetchMembershipStatus(ctx, node)
	}

	// Validate every old member before starting another activation.
	// Retries may encounter a mixture of committed and activated nodes.
	activated := make(map[string]bool)
	for _, node := range current.Nodes() {
		response, err := readStatus(node)
		if err != nil {
			return status.Errorf(
				codes.Unavailable,
				"inspect %s before activation: %v", node.ID, err,
			)
		}

		activated[node.ID] = confirmsJoinActivation(
			response, node.ID, candidateID,
		)
		if !activated[node.ID] &&
			!confirmsJoinCommit(
				response, node.ID, activeID, candidateID,
			) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s has not committed this join", node.ID,
			)
		}
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	// The coordinator retains its old configuration on disk so peers
	// can still query the original commit decision after this switch.
	if err := s.ActivateForJoin(candidate); err != nil {
		return err
	}

	for _, node := range current.Nodes() {
		if node.ID == s.nodeID || activated[node.ID] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}

		response, rpcErr := s.activateJoinPeer(
			ctx, node, current, candidate,
		)
		if rpcErr != nil {
			// Activation may have succeeded before its reply was lost.
			observed, readErr := readStatus(node)
			if readErr != nil ||
				!confirmsJoinActivation(
					observed, node.ID, candidateID,
				) {
				return status.Errorf(
					codes.Unavailable,
					"activation of %s is uncertain: RPC: %v; status: %v",
					node.ID, rpcErr, readErr,
				)
			}
			continue
		}
		if !confirmsJoinActivation(response, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s did not confirm activation", node.ID,
			)
		}
	}

	// Success means every old member reports the candidate and remains paused.
	for _, node := range current.Nodes() {
		response, err := readStatus(node)
		if err != nil {
			return status.Errorf(
				codes.Unavailable,
				"confirm activation on %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinActivation(response, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s has not confirmed activation", node.ID,
			)
		}
	}

	return nil
}
