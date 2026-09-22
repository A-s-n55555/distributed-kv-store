package server

import (
	"errors"
	"fmt"
	"os"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
)

// ConfigurePromotion restores a saved promotion before serving RPCs.
func (s *StagingService) ConfigurePromotion(pausePath string) error {
	if pausePath == "" {
		return fmt.Errorf("promotion pause path is required")
	}
	if s.joinCurrent.Identity().Epoch == 0 {
		return fmt.Errorf("configure the staging join before promotion recovery")
	}

	s.target.writeMu.Lock()
	defer s.target.writeMu.Unlock()
	s.target.replicaApplyMu.Lock()
	defer s.target.replicaApplyMu.Unlock()

	if s.promotionPausePath != "" {
		return fmt.Errorf("promotion recovery is already configured")
	}

	previous, candidate, err := membership.LoadStagingPromotion(
		pausePath, s.target.nodeID,
	)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// No promotion decision: staging transfers may continue.
		s.promotionPausePath = pausePath
		return nil

	case err != nil:
		return fmt.Errorf("restore staging promotion: %w", err)
	}

	if previous.Identity() != s.joinCurrent.Identity() ||
		candidate.Identity() != s.joinCandidate.Identity() {
		return fmt.Errorf("saved promotion differs from configured staging join")
	}

	s.promotionPausePath = pausePath
	s.target.writesPaused = true
	s.target.replicasPaused = true
	return s.installPromotedMembershipLocked()
}
