package server

import (
	"context"
	"log"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// readVersionedQuorum is the single-version interface used by writes.
// Unresolved siblings still block ordinary writes.
func (s *GRPCServer) readVersionedQuorum(
	ctx context.Context,
	key int64,
) (store.Record, bool, error) {
	records, err := s.readSiblingQuorum(ctx, key)
	if err != nil {
		return store.Record{}, false, err
	}

	return selectNewestRecord(records)
}

// readSiblingQuorum preserves all observed non-dominated versions.
func (s *GRPCServer) readSiblingQuorum(
	ctx context.Context,
	key int64,
) ([]store.Record, error) {
	replicaNodes, err := s.replicaNodesFor(key)
	if err != nil {
		return nil, err
	}

	successfulReads := 0
	var records []store.Record
	var lastError error

	observations := make(
		[]replicaObservation,
		0,
		len(replicaNodes),
	)

	for _, node := range replicaNodes {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		replicaRecords, err := s.readRecordsFromReplica(
			ctx,
			node,
			key,
		)
		if err != nil {
			lastError = err
			continue
		}

		// A missing key is a successful replica response too.
		// Count nodes, not versions.
		successfulReads++

		observation := replicaObservation{
			node:    node,
			records: replicaRecords,
			exists:  len(replicaRecords) > 0,
		}

		if len(replicaRecords) == 1 {
			observation.record = replicaRecords[0]
		}

		observations = append(observations, observation)
		records = append(records, replicaRecords...)
	}

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if successfulReads < s.readQuorum {
		return nil, status.Errorf(
			codes.Unavailable,
			"read quorum not reached: successful=%d required=%d: %v",
			successfulReads,
			s.readQuorum,
			lastError,
		)
	}

	versions, err := mergeReplicaVersions(records)
	if err != nil {
		return nil, err
	}

	// Existing repair handles a unique newest version.
	// Do not select one sibling and repair away the others.
	for _, repairError := range s.repairObservedSiblingReplicas(
		ctx,
		key,
		versions,
		observations,
	) {
		log.Printf("read repair failed: %v", repairError)
	}

	return versions, nil
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
