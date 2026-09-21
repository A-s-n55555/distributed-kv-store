package server

import (
	"context"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// preCopyJoinInventory copies and verifies every affected key found
// on either old owner. It does not activate proposed membership.
func (s *GRPCServer) preCopyJoinInventory(
	ctx context.Context,
	proposed *ring.Ring,
) (PreCopyProgress, error) {
	var progress PreCopyProgress

	keys, err := s.collectJoinKeys(ctx)
	if err != nil {
		return progress, err
	}

	var moved []int64
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return progress, err
		}
		diff, err := ring.DiffReplicasForKey(
			s.ring, proposed, key, s.replicationFactor,
		)
		if err != nil {
			return progress, err
		}
		if len(diff.Added) > 0 {
			moved = append(moved, key)
		}
	}
	progress.PlannedKeys = len(moved)

	for _, key := range moved {
		sent, err := s.preCopyKeyFromOldOwners(ctx, proposed, key)
		progress.RecordsSent += sent
		if err != nil {
			return progress, fmt.Errorf(
				"pre-copy inventory key %d: %w", key, err,
			)
		}
		// preCopyKeyFromOldOwners verifies before returning success.
		progress.CompletedKeys++
	}

	return progress, nil
}
