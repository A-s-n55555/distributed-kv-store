package server

import (
	"context"
	"fmt"
	"time"
)

// RunHintDelivery performs sequential delivery passes until canceled.
func (s *GRPCServer) RunHintDelivery(
	ctx context.Context,
	interval time.Duration,
) error {
	if interval <= 0 {
		return fmt.Errorf("hint delivery interval must be positive")
	}

	if s.hintQueue == nil {
		return fmt.Errorf("hint queue is not configured")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}

		if _, err := s.deliverPendingHints(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			// Begin another delivery pass.
		}
	}
}
