package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Existing Put/Delete callers keep their current interface.
func (s *GRPCServer) coordinateVersionedWrite(
	ctx context.Context,
	key int64,
	value string,
	deleted bool,
) error {
	return s.coordinateWrite(
		ctx, key, value, deleted, nil, false,
	)
}

func (s *GRPCServer) coordinateWrite(
	ctx context.Context,
	key int64,
	value string,
	deleted bool,
	suppliedContext version.Clock,
	resolving bool,
) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.writesPaused {
		return status.Error(codes.Unavailable, "writes temporarily paused for membership transition")
	}

	if deleted && value != "" {
		return status.Error(
			codes.InvalidArgument,
			"a deletion must have an empty value",
		)
	}

	replicaNodes, err := s.replicaNodesFor(key)
	if err != nil {
		return err
	}

	var observed version.Clock

	if resolving {
		// Read all observed siblings, not a single winner.
		records, err := s.readSiblingQuorum(ctx, key)
		if err != nil {
			return err
		}

		observed, err = checkedResolutionContext(
			suppliedContext,
			records,
		)
		if err != nil {
			return err
		}
	} else {
		// Ordinary writes still reject unresolved siblings.
		current, _, err := s.readVersionedQuorum(ctx, key)
		if err != nil {
			return err
		}

		observed = current.Clock
	}

	clock, err := s.store.NextClock(s.nodeID, observed)
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

		successfulWrites++
	}
	if hintPersistenceError != nil {
		return status.Errorf(
			codes.Internal,
			"failed to preserve pending replica delivery: %v", hintPersistenceError,
		)
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
