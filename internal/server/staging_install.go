package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
)

// installPromotedMembershipLocked finishes a saved promotion.
// Caller must hold target.writeMu, then target.replicaApplyMu.
func (s *StagingService) installPromotedMembershipLocked() error {
	if !s.target.writesPaused || !s.target.replicasPaused {
		return fmt.Errorf("promotion installation requires both gates closed")
	}

	previous, candidate, err := membership.LoadStagingPromotion(
		s.promotionPausePath, s.target.nodeID,
	)
	if err != nil {
		return err
	}
	if previous.Identity() != s.joinCurrent.Identity() ||
		candidate.Identity() != s.joinCandidate.Identity() {
		return fmt.Errorf("saved promotion differs from configured join")
	}

	actual, err := membership.NewConfiguration(
		candidate.Identity().Epoch,
		s.target.ring,
		s.target.replicationFactor,
	)
	if err != nil || actual.Identity() != candidate.Identity() {
		return fmt.Errorf("staging server ring differs from candidate")
	}
	if s.target.readQuorum < 1 ||
		s.target.readQuorum > candidate.ReplicationFactor() ||
		s.target.writeQuorum < 1 ||
		s.target.writeQuorum > candidate.ReplicationFactor() {
		return fmt.Errorf("staging server quorums are invalid")
	}

	activePath := filepath.Join(
		filepath.Dir(s.promotionPausePath),
		"membership-active.json",
	)

	savedActive, err := membership.LoadCandidate(activePath)
	switch {
	case err == nil:
		if savedActive.Identity() != candidate.Identity() {
			return fmt.Errorf("active membership belongs to another configuration")
		}
	case errors.Is(err, os.ErrNotExist):
		// First installation.
	default:
		return fmt.Errorf("inspect active membership: %w", err)
	}

	// Recovery can repeat these operations after any interrupted write.
	if err := membership.SaveJoinPause(
		s.promotionPausePath, previous, candidate,
	); err != nil {
		return fmt.Errorf("save promoted pause: %w", err)
	}
	if err := membership.RecordJoinCommit(
		s.promotionPausePath, previous, candidate,
	); err != nil {
		return fmt.Errorf("save promoted commit: %w", err)
	}
	if err := membership.SaveCandidate(activePath, candidate); err != nil {
		return fmt.Errorf("save promoted active membership: %w", err)
	}

	identity := candidate.Identity()
	s.target.activeMembership = candidate
	s.target.pendingJoin = &identity
	s.target.joinPausePath = s.promotionPausePath
	return s.target.restoreReplicaReadinessLocked()
}
