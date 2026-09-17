package server

import (
	"context"
	"log"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) readVersionedQuorum(
	ctx context.Context,
	key int64,
) (store.Record, bool, error) {
	replicaNodes, err := s.replicaNodesFor(key)
	if err != nil {
		return store.Record{}, false, err
	}

	successfulReads := 0
	records := make([]store.Record, 0, len(replicaNodes))
	observations := make(
		[]replicaObservation,
		0,
		len(replicaNodes),
	)

	var lastError error

	for _, node := range replicaNodes {
		record, exists, err := s.readRecordFromReplica(
			ctx,
			node,
			key,
		)
		if err != nil {
			lastError = err
			continue
		}

		successfulReads++

		observations = append(
			observations,
			replicaObservation{
				node:   node,
				record: record,
				exists: exists,
			},
		)

		if exists {
			records = append(records, record)
		}
	}

	if successfulReads < s.readQuorum {
		return store.Record{}, false, status.Errorf(
			codes.Unavailable,
			"read quorum not reached: successful=%d required=%d: %v",
			successfulReads,
			s.readQuorum,
			lastError,
		)
	}

	selected, exists, err := selectNewestRecord(records)
	if err != nil {
		return store.Record{}, false, err
	}

	if !exists {
		return store.Record{}, false, nil
	}

	repairErrors := s.repairObservedReplicas(
		ctx,
		key,
		selected,
		observations,
	)

	for _, repairError := range repairErrors {
		log.Printf("read repair failed: %v", repairError)
	}

	return selected, true, nil
}

func selectNewestRecord(
	records []store.Record,
) (store.Record, bool, error) {
	if len(records) == 0 {
		return store.Record{}, false, nil
	}

	candidate := records[0]

	// Find a possible record that dominates all others.
	for _, record := range records[1:] {
		if version.Compare(record.Clock, candidate.Clock) ==
			version.After {
			candidate = record
		}
	}

	// Verify that the candidate really dominates or equals every record.
	for _, record := range records {
		switch version.Compare(candidate.Clock, record.Clock) {
		case version.After:
			// The candidate includes this older version.

		case version.Equal:
			if candidate.Value != record.Value ||
				candidate.Deleted != record.Deleted {
				return store.Record{}, false, status.Error(
					codes.Aborted,
					"equal clocks have different record contents",
				)
			}

		default:
			return store.Record{}, false, status.Error(
				codes.Aborted,
				"replicas contain unresolved concurrent versions",
			)
		}
	}

	if candidate.Clock != nil {
		candidate.Clock = version.Clone(candidate.Clock)
	}

	return candidate, true, nil
}
