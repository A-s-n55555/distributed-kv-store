package server

import (
	"context"
	"fmt"
	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

type PreCopyProgress struct {
	PlannedKeys   int
	CompletedKeys int
	RecordsSent   int
}

// preCopyPlannedKeys copies keys found in this node's current snapshot.
// It does not catch writes made after the snapshot.
func (s *GRPCServer) preCopyPlannedKeys(
	ctx context.Context,
	proposed *ring.Ring,
) (PreCopyProgress, error) {
	var progress PreCopyProgress

	if err := ctx.Err(); err != nil {
		return progress, err
	}
	if s.store == nil {
		return progress, fmt.Errorf("local store is required")
	}

	snapshot := s.store.Snapshot()
	transfers, err := membership.PlanLocalTransfers(
		snapshot, s.ring, proposed, s.nodeID, s.replicationFactor,
	)
	if err != nil {
		return progress, err
	}
	progress.PlannedKeys = len(transfers)

	// Fail before network writes if a planned key cannot use
	// ApplyReplicaRecord's versioned-record format.
	for _, transfer := range transfers {
		for _, record := range snapshot[transfer.Key] {
			if len(record.Clock) == 0 {
				return progress, fmt.Errorf(
					"key %d has a legacy record without a vector clock",
					transfer.Key,
				)
			}
		}
	}

	for _, transfer := range transfers {
		sent, err := s.preCopyKey(ctx, proposed, transfer.Key)
		progress.RecordsSent += sent
		if err != nil {
			return progress, fmt.Errorf(
				"pre-copy planned key %d: %w", transfer.Key, err,
			)
		}
		progress.CompletedKeys++
	}

	return progress, nil
}

// preCopyKey sends this old owner's current versions to newly assigned owners.
// Success does not authorize a membership switch; later writes need catch-up.
func (s *GRPCServer) preCopyKey(
	ctx context.Context,
	proposed *ring.Ring,
	key int64,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.store == nil {
		return 0, fmt.Errorf("local store is required")
	}

	diff, err := ring.DiffReplicasForKey(
		s.ring, proposed, key, s.replicationFactor,
	)
	if err != nil {
		return 0, err
	}

	isOldOwner := false
	for _, node := range diff.Old {
		if node.ID == s.nodeID {
			isOldOwner = true
			break
		}
	}
	if !isOldOwner {
		return 0, fmt.Errorf("node %q is not an old owner of key %d",
			s.nodeID, key)
	}
	if len(diff.Added) == 0 {
		return 0, nil
	}

	records := s.store.GetRecords(key)
	if len(records) == 0 {
		return 0, fmt.Errorf("old owner %q has no versions for key %d",
			s.nodeID, key)
	}

	// Check every record before sending any. ApplyReplicaRecord requires
	// vector clocks and cannot transfer legacy unversioned records.
	for _, record := range records {
		if len(record.Clock) == 0 {
			return 0, fmt.Errorf(
				"key %d has a legacy record without a vector clock", key,
			)
		}
	}

	sent := 0
	for _, destination := range diff.Added {
		for _, record := range records {
			if err := s.applyRecordToReplica(
				ctx, destination, key, record,
			); err != nil {
				return sent, fmt.Errorf(
					"pre-copy key %d to %s: %w",
					key, destination.ID, err,
				)
			}
			sent++
		}
	}
	return sent, nil
}
