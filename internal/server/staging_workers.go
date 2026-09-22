package server

import (
	"context"
	"fmt"
	"time"
)

// RunPromotedWorkers waits for promotion and open request gates,
// then runs normal background services until cancellation.
// Call once from the staging process's main lifecycle.
func (s *StagingService) RunPromotedWorkers(
	ctx context.Context,
	antiEntropyInterval time.Duration,
) error {
	if ctx == nil {
		return fmt.Errorf("worker context is required")
	}
	if antiEntropyInterval <= 0 {
		return fmt.Errorf("anti-entropy interval must be positive")
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		s.target.writeMu.Lock()
		s.target.replicaApplyMu.Lock()

		expected := s.joinCandidate.Identity()
		ready := expected.Epoch != 0 &&
			s.target.activeMembership.Identity() == expected &&
			!s.target.writesPaused &&
			!s.target.replicasPaused

		s.target.replicaApplyMu.Unlock()
		s.target.writeMu.Unlock()

		if ready {
			break
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}

	if s.target.hintQueue == nil {
		return fmt.Errorf("promoted node requires a hint queue")
	}
	if ctx.Err() != nil {
		return nil
	}

	if err := s.target.StartAntiEntropy(
		ctx, antiEntropyInterval,
	); err != nil {
		return err
	}
	defer s.target.StopAntiEntropy()

	return s.target.RunHintDelivery(ctx, 3*time.Second)
}
