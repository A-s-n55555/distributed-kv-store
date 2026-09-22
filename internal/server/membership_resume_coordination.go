package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// prepareJoinResume records the resume decision after all candidate members
// confirm activation. A saved decision makes retries independent of peers.
func (s *GRPCServer) prepareJoinResume(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	s.writeMu.Lock()
	active := s.activeMembership
	path := s.joinPausePath
	s.writeMu.Unlock()

	if active.Identity() != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator has not activated this candidate",
		)
	}

	previous, err := membership.LoadActivatedJoin(path, active)
	if err != nil {
		return status.Errorf(
			codes.FailedPrecondition,
			"load activated join: %v", err,
		)
	}
	if err := s.requireJoinCoordinator(previous); err != nil {
		return err
	}

	recorded, err := membership.LoadJoinResumeDecision(path, active)
	if err != nil {
		return status.Errorf(
			codes.Internal, "inspect resume decision: %v", err,
		)
	}
	if recorded {
		return nil
	}

	// Include the joining node, not just the old membership.
	for _, node := range candidate.Nodes() {
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
				codes.Unavailable,
				"check %s before resume: %v", node.ID, err,
			)
		}
		if !confirmsJoinActivation(
			response, node.ID, candidate.Identity(),
		) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s is not activated and paused", node.ID,
			)
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.activeMembership.Identity() != candidate.Identity() ||
		s.joinPausePath != path {
		return status.Error(
			codes.FailedPrecondition,
			"local membership changed during resume verification",
		)
	}

	// Another concurrent attempt may already have saved the decision.
	recorded, err = membership.LoadJoinResumeDecision(path, candidate)
	if err != nil {
		return status.Errorf(
			codes.Internal, "recheck resume decision: %v", err,
		)
	}
	if recorded {
		return nil
	}

	if !s.writesPaused || !s.replicasPaused ||
		s.pendingJoin == nil ||
		*s.pendingJoin != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator is not paused for this join",
		)
	}

	if err := membership.RecordJoinResumeDecision(
		path, s.nodeID, candidate,
	); err != nil {
		return status.Errorf(
			codes.Internal, "record resume decision: %v", err,
		)
	}
	return nil
}
