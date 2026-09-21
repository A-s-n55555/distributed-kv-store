package server

import (
	"context"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
)

// preCopyKeyFromOldOwners gathers all old-owner versions before sending.
// The returned count is successful record RPCs, including duplicates on retry.
func (s *GRPCServer) preCopyKeyFromOldOwners(
	ctx context.Context,
	proposed *ring.Ring,
	key int64,
) (int, error) {
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

	var observed []store.Record
	for _, node := range diff.Old {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		records, err := s.readRecordsFromReplica(ctx, node, key)
		if err != nil {
			return 0, fmt.Errorf(
				"read old owner %s for key %d: %w", node.ID, key, err,
			)
		}
		for _, record := range records {
			if len(record.Clock) == 0 {
				return 0, fmt.Errorf(
					"old owner %s has an unversioned record for key %d",
					node.ID, key,
				)
			}
		}
		observed = append(observed, records...)
	}
	if len(observed) == 0 {
		return 0, fmt.Errorf("no old owner has versions for key %d", key)
	}

	versions, err := mergeReplicaVersions(observed)
	if err != nil {
		return 0, fmt.Errorf("merge versions for key %d: %w", key, err)
	}

	sent := 0
	for _, destination := range diff.Added {
		for _, record := range versions {
			if err := s.applyRecordToReplica(
				ctx, destination, key, record,
			); err != nil {
				return sent, fmt.Errorf(
					"copy key %d to %s: %w",
					key, destination.ID, err,
				)
			}
			sent++
		}
	}

	if err := s.verifyPreCopyKey(ctx, proposed, key); err != nil {
		return sent, fmt.Errorf("verify copied key %d: %w", key, err)
	}
	return sent, nil
}
