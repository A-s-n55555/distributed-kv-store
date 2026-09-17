package server

import (
	"context"
	"fmt"
	"log"
)

// deliverPendingHints performs one pass over a pending-hint snapshot.
// Replica delivery failures remain pending and are logged.
// Queue acknowledgment failures stop the pass.
func (s *GRPCServer) deliverPendingHints(
	ctx context.Context,
) (int, error) {
	if s.hintQueue == nil {
		return 0, fmt.Errorf("hint queue is not configured")
	}

	delivered := 0

	for _, hint := range s.hintQueue.Pending() {
		if err := ctx.Err(); err != nil {
			return delivered, err
		}

		if err := s.applyRecordToReplica(
			ctx,
			hint.Target,
			hint.Key,
			hint.Record,
		); err != nil {
			log.Printf(
				"hint %s delivery to %s failed; retained: %v",
				hint.ID,
				hint.Target.ID,
				err,
			)

			continue
		}

		// Remove only after successful target processing.
		if err := s.hintQueue.Ack(hint.ID); err != nil {
			return delivered, fmt.Errorf(
				"acknowledge hint %s: %w",
				hint.ID,
				err,
			)
		}

		delivered++

		log.Printf(
			"delivered hint %s for key %d to node %s",
			hint.ID,
			hint.Key,
			hint.Target.ID,
		)
	}

	return delivered, nil
}
