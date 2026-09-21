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

func confirmsJoinPause(
	response *kvpb.MembershipStatusResponse,
	nodeID string,
	active, candidate membership.Identity,
) bool {
	return response != nil &&
		response.GetNodeId() == nodeID &&
		response.GetActiveEpoch() == active.Epoch &&
		bytes.Equal(response.GetActiveDigest(), active.Digest[:]) &&
		response.GetWritesPaused() &&
		response.GetPendingEpoch() == candidate.Epoch &&
		bytes.Equal(response.GetPendingDigest(), candidate.Digest[:])
}

// pauseOldMembersForJoin pauses remote old members first and this node last.
// An error can leave some members paused; retry with the same candidate.
func (s *GRPCServer) pauseOldMembersForJoin(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := s.checkJoinReadiness(ctx, candidate); err != nil {
		return err
	}

	s.writeMu.Lock()
	current := s.activeMembership
	s.writeMu.Unlock()

	activeID := current.Identity()
	candidateID := candidate.Identity()
	nodes := current.Nodes()

	for _, node := range nodes {
		if node.ID == s.nodeID {
			continue
		}

		if _, err := s.pauseJoinPeer(ctx, node, current, candidate); err != nil {
			// The reply could have been lost after the peer saved its pause.
			observed, readErr := s.fetchMembershipStatus(ctx, node)
			if readErr != nil || !confirmsJoinPause(
				observed, node.ID, activeID, candidateID,
			) {
				return status.Errorf(
					codes.Unavailable,
					"pause of %s is uncertain: RPC: %v; status read: %v",
					node.ID, err, readErr,
				)
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if err := s.PauseForJoin(candidate); err != nil {
		return err
	}

	// Confirm the state observed after every pause request completed.
	for _, node := range nodes {
		var response *kvpb.MembershipStatusResponse
		var err error

		if node.ID == s.nodeID {
			response, err = s.GetMembershipStatus(
				ctx, &kvpb.MembershipStatusRequest{},
			)
		} else {
			response, err = s.fetchMembershipStatus(ctx, ring.Node{
				ID: node.ID, Address: node.Address,
			})
		}
		if err != nil {
			return status.Errorf(
				codes.Unavailable, "confirm pause of %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinPause(response, node.ID, activeID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s did not confirm the join pause", node.ID,
			)
		}
	}

	return nil
}
