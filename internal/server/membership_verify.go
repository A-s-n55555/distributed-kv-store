package server

import (
	"context"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
)

// verifyPreCopyKey compares each new owner with the merged versions
// currently readable from every old owner.
func (s *GRPCServer) verifyPreCopyKey(
	ctx context.Context,
	proposed *ring.Ring,
	key int64,
) error {
	diff, err := ring.DiffReplicasForKey(
		s.ring, proposed, key, s.replicationFactor,
	)
	if err != nil {
		return err
	}
	if len(diff.Added) == 0 {
		return nil
	}

	var oldRecords []store.Record
	for _, node := range diff.Old {
		if err := ctx.Err(); err != nil {
			return err
		}

		records, err := s.readRecordsFromReplica(ctx, node, key)
		if err != nil {
			return fmt.Errorf("read old owner %s for key %d: %w",
				node.ID, key, err)
		}
		for _, record := range records {
			if len(record.Clock) == 0 {
				return fmt.Errorf("old owner %s has an unversioned record for key %d",
					node.ID, key)
			}
		}
		oldRecords = append(oldRecords, records...)
	}
	if len(oldRecords) == 0 {
		return fmt.Errorf("no old owner has versions for key %d", key)
	}

	merged, err := mergeReplicaVersions(oldRecords)
	if err != nil {
		return fmt.Errorf("merge old versions for key %d: %w", key, err)
	}
	expected := antientropy.KeyDigest(key, merged)

	for _, node := range diff.Added {
		if err := ctx.Err(); err != nil {
			return err
		}

		records, err := s.readRecordsFromReplica(ctx, node, key)
		if err != nil {
			return fmt.Errorf("read new owner %s for key %d: %w",
				node.ID, key, err)
		}
		for _, record := range records {
			if len(record.Clock) == 0 {
				return fmt.Errorf("new owner %s has an unversioned record for key %d",
					node.ID, key)
			}
		}

		if antientropy.KeyDigest(key, records) != expected {
			return fmt.Errorf(
				"new owner %s does not match old owners for key %d",
				node.ID, key,
			)
		}
	}
	return nil
}
