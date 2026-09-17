package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) coordinateVersionedWrite(
	ctx context.Context,
	key int64,
	value string,
	deleted bool,
) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	replicaNodes, err := s.replicaNodesFor(key)
	if err != nil {
		return err
	}

	current, _, err := s.readVersionedQuorum(ctx, key)
	if err != nil {
		return err
	}

	clock, err := s.store.NextClock(s.nodeID, current.Clock)
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"reserve write clock failed: %v",
			err,
		)
	}

	incoming := store.Record{
		Value:   value,
		Clock:   clock,
		Deleted: deleted,
	}

	successfulWrites := 0
	var lastError error
	var conflictError error
	var hintPersistenceError error

	for _, node := range replicaNodes {
		err := s.applyRecordToReplica(ctx, node, key, incoming)
		if err != nil {
			lastError = err

			if status.Code(err) == codes.Aborted {
				conflictError = err
			}

			if hintError := s.saveFailedReplicaHint(
				node,
				key,
				incoming,
				err,
			); hintError != nil {
				hintPersistenceError = hintError
			}

			continue
		}
		if hintPersistenceError != nil {
			return status.Errorf(
				codes.Internal,
				"failed to preserve pending replica delivery: %v",
				hintPersistenceError,
			)
		}

		successfulWrites++
	}

	// Conservatively report any detected record conflict,
	// even if enough other replicas acknowledged the operation.
	if conflictError != nil {
		return status.Errorf(
			codes.Aborted,
			"replicated write encountered a conflict: %v",
			conflictError,
		)
	}

	if successfulWrites < s.writeQuorum {
		return status.Errorf(
			codes.Unavailable,
			"write quorum not reached: successful=%d required=%d: %v",
			successfulWrites,
			s.writeQuorum,
			lastError,
		)
	}

	return nil
}
