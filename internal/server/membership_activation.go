package server

import (
	"path/filepath"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ActivateForJoin installs a committed membership while both gates stay closed.
func (s *GRPCServer) ActivateForJoin(
	candidate membership.Configuration,
) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if s.ring == nil || s.joinPausePath == "" {
		return status.Error(
			codes.FailedPrecondition,
			"ring and join pause path must be configured",
		)
	}
	if !s.writesPaused || !s.replicasPaused ||
		s.pendingJoin == nil ||
		*s.pendingJoin != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"node is not paused for this candidate",
		)
	}

	active := s.activeMembership
	actual, err := membership.NewConfiguration(
		active.Identity().Epoch, s.ring, s.replicationFactor,
	)
	if err != nil || actual.Identity() != active.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"server ring differs from active membership",
		)
	}

	previous := active
	if active.Identity() == candidate.Identity() {
		// Retry after live activation or restart.
		previous, err = membership.LoadActivatedJoin(
			s.joinPausePath, active,
		)
		if err != nil {
			return status.Errorf(
				codes.FailedPrecondition,
				"recover activated join: %v", err,
			)
		}
	}

	joining, err := membership.ValidateJoinCandidate(previous, candidate)
	if err != nil {
		return status.Errorf(
			codes.FailedPrecondition,
			"invalid activation candidate: %v", err,
		)
	}

	// main.go stores both membership files in the same node directory.
	activePath := filepath.Join(
		filepath.Dir(s.joinPausePath),
		"membership-active.json",
	)
	if err := membership.ActivateCommittedJoin(
		activePath, s.joinPausePath, previous, candidate,
	); err != nil {
		return status.Errorf(
			codes.FailedPrecondition,
			"persist activation: %v", err,
		)
	}

	// AddNode holds the ring's own mutex. Keep the ring pointer stable
	// because background workers also read it.
	s.ring.AddNode(joining)
	s.activeMembership = candidate

	// Preserve pendingJoin, writesPaused, and replicasPaused.
	return nil
}
