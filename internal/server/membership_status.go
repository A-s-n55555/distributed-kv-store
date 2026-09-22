package server

import (
	"context"
	"errors"
	"os"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetMembershipStatus reports membership and verified local gate state.
func (s *GRPCServer) GetMembershipStatus(
	ctx context.Context,
	request *kvpb.MembershipStatusRequest,
) (*kvpb.MembershipStatusResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	active := s.activeMembership.Identity()
	if active.Epoch == 0 {
		return nil, status.Error(
			codes.FailedPrecondition,
			"active membership is not configured",
		)
	}

	replicasReady := false
	if s.joinPausePath != "" {
		var err error
		replicasReady, err = membership.LoadJoinReplicaReady(
			s.joinPausePath, s.activeMembership,
		)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal, "inspect replica readiness: %v", err,
			)
		}
	}

	clientsReleased := false
	if s.joinPausePath != "" {
		var err error
		clientsReleased, err = membership.LoadJoinClientsReleased(
			s.joinPausePath, s.activeMembership,
		)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal, "inspect client release: %v", err,
			)
		}
	}

	if clientsReleased {
		if !replicasReady || s.writesPaused || s.replicasPaused ||
			s.pendingJoin == nil || *s.pendingJoin != active {
			return nil, status.Error(
				codes.FailedPrecondition,
				"client-release marker disagrees with local gates or membership",
			)
		}
	} else if replicasReady {
		if !s.writesPaused || s.replicasPaused ||
			s.pendingJoin == nil || *s.pendingJoin != active {
			return nil, status.Error(
				codes.FailedPrecondition,
				"replica-ready marker disagrees with local gates or membership",
			)
		}
	} else if s.writesPaused != s.replicasPaused {
		return nil, status.Error(
			codes.FailedPrecondition,
			"local gates disagree without replica readiness",
		)
	}

	response := &kvpb.MembershipStatusResponse{
		NodeId:          s.nodeID,
		ActiveEpoch:     active.Epoch,
		ActiveDigest:    append([]byte(nil), active.Digest[:]...),
		WritesPaused:    s.writesPaused,
		ReplicasReady:   replicasReady,
		ClientsReleased: clientsReleased,
	}
	if s.pendingJoin != nil {
		response.PendingEpoch = s.pendingJoin.Epoch
		response.PendingDigest = append(
			[]byte(nil), s.pendingJoin.Digest[:]...,
		)
	}

	if s.joinPausePath == "" {
		return response, nil
	}

	if s.pendingJoin != nil && *s.pendingJoin == active {
		if _, err := membership.LoadActivatedJoin(
			s.joinPausePath, s.activeMembership,
		); err != nil {
			return nil, status.Errorf(
				codes.Internal, "inspect activated join: %v", err,
			)
		}
		if !s.writesPaused && !clientsReleased {
			return nil, status.Error(
				codes.FailedPrecondition,
				"activated join has clients unexpectedly enabled",
			)
		}
		response.JoinCommitted = true
		return response, nil
	}

	committed, err := membership.LoadJoinCommit(
		s.joinPausePath, s.activeMembership,
	)
	switch {
	case err == nil:
		if !s.writesPaused || !s.replicasPaused ||
			s.pendingJoin == nil ||
			*s.pendingJoin != committed.Identity() {
			return nil, status.Error(
				codes.FailedPrecondition,
				"committed join is not paused for its candidate",
			)
		}
		response.JoinCommitted = true

	case errors.Is(err, os.ErrNotExist):
		// No commit has been recorded.

	default:
		return nil, status.Errorf(
			codes.Internal, "inspect join commitment: %v", err,
		)
	}

	return response, nil
}
