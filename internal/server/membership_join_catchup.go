package server

import (
	"context"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
)

// CatchUpJoin copies and verifies changes after the old members are paused.
// On error, the caller must retry or explicitly abort the join.
func (s *GRPCServer) CatchUpJoin(
	ctx context.Context,
	candidate membership.Configuration,
) (PreCopyProgress, error) {
	var progress PreCopyProgress

	if err := s.pauseOldMembersForJoin(ctx, candidate); err != nil {
		return progress, err
	}

	progress, err := s.preCopyJoinInventory(ctx, candidate.Ring())
	if err != nil {
		return progress, fmt.Errorf("copy paused join inventory: %w", err)
	}

	if err := s.pauseOldMembersForJoin(ctx, candidate); err != nil {
		return progress, fmt.Errorf("reconfirm join pause after copy: %w", err)
	}
	return progress, nil
}
