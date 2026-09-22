package server

import (
	"errors"
	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
)

// SetActiveMembership gives the server the configuration validated at startup.
func (s *GRPCServer) SetActiveMembership(active membership.Configuration) error {
	if s.ring == nil {
		return status.Error(codes.FailedPrecondition, "cluster ring is required")
	}

	actual, err := membership.NewConfiguration(
		active.Identity().Epoch, s.ring, s.replicationFactor,
	)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid active membership: %v", err)
	}
	if actual.Identity() != active.Identity() {
		return status.Error(codes.FailedPrecondition, "active membership differs from server ring")
	}

	localMember := false
	for _, node := range active.Nodes() {
		if node.ID == s.nodeID {
			localMember = true
			break
		}
	}
	if !localMember {
		return status.Error(codes.FailedPrecondition, "local node is absent from active membership")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.writesPaused {
		return status.Error(codes.FailedPrecondition, "cannot set membership while writes are paused")
	}
	s.activeMembership = active
	return nil
}

// ConfigureJoinPause restores a durable pause before this server accepts RPCs.
func (s *GRPCServer) ConfigureJoinPause(path string) error {
	if path == "" {
		return status.Error(codes.InvalidArgument, "join pause path is required")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if s.activeMembership.Identity().Epoch == 0 {
		return status.Error(codes.FailedPrecondition, "active membership is not configured")
	}
	if s.joinPausePath != "" {
		return status.Error(codes.FailedPrecondition, "join pause path is already configured")
	}

	candidate, err := membership.LoadJoinPause(path, s.activeMembership)

	previous, previousErr := membership.LoadPreviousJoin(path)
	switch {
	case previousErr == nil && previous.Identity() != s.activeMembership.Identity():
		// An active file that advanced must have valid pause and commit
		// markers. Any missing or corrupt marker prevents startup.
		if _, recoveryErr := membership.LoadActivatedJoin(
			path, s.activeMembership,
		); recoveryErr != nil {
			return status.Errorf(
				codes.FailedPrecondition,
				"invalid activated join: %v", recoveryErr,
			)
		}
		candidate = s.activeMembership
		err = nil

	case previousErr != nil && !errors.Is(previousErr, os.ErrNotExist):
		return status.Errorf(
			codes.FailedPrecondition,
			"invalid previous membership: %v", previousErr,
		)

	case err != nil && !errors.Is(err, os.ErrNotExist):
		return status.Errorf(
			codes.FailedPrecondition,
			"invalid saved join pause: %v", err,
		)
	}
	s.joinPausePath = path
	if err == nil {
		identity := candidate.Identity()
		s.pendingJoin = &identity
		s.writesPaused = true
		s.replicasPaused = true
	}
	return s.restoreReplicaReadinessLocked()
}

// PauseForJoin validates one proposed join, drains local write paths,
// and associates the pause with that candidate's identity.
func (s *GRPCServer) PauseForJoin(candidate membership.Configuration) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current := s.activeMembership
	if current.Identity().Epoch == 0 {
		return status.Error(codes.FailedPrecondition, "active membership is not configured")
	}

	actual, err := membership.NewConfiguration(
		current.Identity().Epoch, s.ring, s.replicationFactor,
	)
	if err != nil || actual.Identity() != current.Identity() {
		return status.Error(codes.FailedPrecondition, "server ring differs from active membership")
	}
	if _, err := membership.ValidateJoinCandidate(current, candidate); err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid join candidate: %v", err)
	}

	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if s.joinPausePath == "" {
		return status.Error(codes.FailedPrecondition, "join pause path is not configured")
	}
	aborted, err := membership.JoinWasAborted(
		s.joinPausePath, current, candidate,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "check join abort marker: %v", err)
	}
	if aborted {
		return status.Error(codes.FailedPrecondition, "join candidate was aborted")
	}

	identity := candidate.Identity()
	if s.writesPaused || s.replicasPaused {
		if s.pendingJoin == nil || *s.pendingJoin != identity {
			return status.Error(codes.FailedPrecondition, "node is paused for another operation")
		}
		// A retry also checks that its durable marker is valid.
		if err := membership.SaveJoinPause(
			s.joinPausePath, current, candidate,
		); err != nil {
			return status.Errorf(codes.Internal, "persist join pause: %v", err)
		}
		return nil
	}

	// Both gates are held here. Persist first; only then acknowledge the pause.
	if err := membership.SaveJoinPause(
		s.joinPausePath, current, candidate,
	); err != nil {
		return status.Errorf(codes.Internal, "persist join pause: %v", err)
	}

	s.writesPaused = true
	s.replicasPaused = true
	s.pendingJoin = &identity
	return nil
}

// ResumeForJoin aborts the exact join for which this node is paused.
func (s *GRPCServer) ResumeForJoin(identity membership.Identity) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if s.pendingJoin == nil || *s.pendingJoin != identity ||
		!s.writesPaused || !s.replicasPaused {
		return status.Error(codes.FailedPrecondition, "join pause identity does not match")
	}

	candidate, err := membership.LoadJoinPause(
		s.joinPausePath, s.activeMembership,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "load join pause: %v", err)
	}
	if candidate.Identity() != identity {
		return status.Error(codes.FailedPrecondition, "saved join pause identity does not match")
	}

	return s.abortForJoinLocked(candidate)
}

// AbortForJoin records the abort even if a pause request has not arrived yet.
func (s *GRPCServer) AbortForJoin(candidate membership.Configuration) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	return s.abortForJoinLocked(candidate)
}

// abortForJoinLocked requires writeMu, then replicaApplyMu.
func (s *GRPCServer) abortForJoinLocked(candidate membership.Configuration) error {
	current := s.activeMembership
	if current.Identity().Epoch == 0 {
		return status.Error(codes.FailedPrecondition, "active membership is not configured")
	}
	if s.joinPausePath == "" {
		return status.Error(codes.FailedPrecondition, "join pause path is not configured")
	}

	if _, err := membership.LoadJoinCommit(
		s.joinPausePath, current,
	); err == nil {
		return status.Error(
			codes.FailedPrecondition,
			"committed join cannot be aborted",
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.Internal, "inspect join commitment: %v", err)
	}

	actual, err := membership.NewConfiguration(
		current.Identity().Epoch, s.ring, s.replicationFactor,
	)
	if err != nil || actual.Identity() != current.Identity() {
		return status.Error(codes.FailedPrecondition, "server ring differs from active membership")
	}
	if _, err := membership.ValidateJoinCandidate(current, candidate); err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid join candidate: %v", err)
	}

	identity := candidate.Identity()
	paused := s.writesPaused && s.replicasPaused &&
		s.pendingJoin != nil && *s.pendingJoin == identity
	if !paused &&
		(s.writesPaused || s.replicasPaused || s.pendingJoin != nil) {
		return status.Error(codes.FailedPrecondition, "node is paused for another operation")
	}

	// An unpaused node must also check its durable state before confirming abort.
	savedPause := false
	if !paused {
		saved, err := membership.LoadJoinPause(s.joinPausePath, current)
		switch {
		case err == nil:
			if saved.Identity() != identity {
				return status.Error(codes.FailedPrecondition, "saved pause belongs to another join")
			}
			savedPause = true
		case errors.Is(err, os.ErrNotExist):
			// A pause request may still be in flight.
		default:
			return status.Errorf(codes.Internal, "load join pause: %v", err)
		}
	}

	// Persist the fence before removing any saved pause or opening either gate.
	if err := membership.RecordJoinAbort(
		s.joinPausePath, current, candidate,
	); err != nil {
		return status.Errorf(codes.Internal, "persist join abort: %v", err)
	}

	if paused || savedPause {
		if err := membership.ClearJoinPause(
			s.joinPausePath, current, identity,
		); err != nil {
			return status.Errorf(codes.Internal, "clear join pause: %v", err)
		}
	}

	if paused {
		s.pendingJoin = nil
		s.replicasPaused = false
		s.writesPaused = false
	}
	return nil
}
