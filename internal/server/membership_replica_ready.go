package server

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
)

// restoreReplicaReadinessLocked restores replica readiness and client release.
// Caller must hold writeMu, then replicaApplyMu.
func (s *GRPCServer) restoreReplicaReadinessLocked() error {
	ready, err := membership.LoadJoinReplicaReady(
		s.joinPausePath, s.activeMembership,
	)
	if err != nil {
		return fmt.Errorf("restore replica readiness: %w", err)
	}

	released, err := membership.LoadJoinClientsReleased(
		s.joinPausePath, s.activeMembership,
	)
	if err != nil {
		return fmt.Errorf("restore client release: %w", err)
	}

	if !ready {
		// LoadJoinClientsReleased rejects a release without readiness.
		return nil
	}

	if s.pendingJoin == nil ||
		*s.pendingJoin != s.activeMembership.Identity() {
		return fmt.Errorf("readiness belongs to a different pending join")
	}
	if !released && !s.writesPaused {
		return fmt.Errorf("clients are open without a durable release")
	}

	// Validate all markers before changing either gate.
	s.replicasPaused = false
	s.writesPaused = !released
	return nil
}
