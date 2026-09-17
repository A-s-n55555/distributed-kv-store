package server

import (
	"context"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

// replicaObservation describes a successful replica read.
// exists=false means the replica responded but had no record.
type replicaObservation struct {
	node   ring.Node
	record store.Record
	exists bool
}

func needsReadRepair(
	selected store.Record,
	observed store.Record,
	exists bool,
) bool {
	// Legacy records have no version suitable for ApplyRecord().
	if len(selected.Clock) == 0 {
		return false
	}

	if !exists {
		return true
	}

	return version.Compare(
		observed.Clock,
		selected.Clock,
	) == version.Before
}

// repairObservedReplicas attempts repair without allocating a new clock.
// Errors are returned so the read coordinator can report/log them.
func (s *GRPCServer) repairObservedReplicas(
	ctx context.Context,
	key int64,
	selected store.Record,
	observations []replicaObservation,
) []error {
	var repairErrors []error

	for _, observation := range observations {
		if !needsReadRepair(
			selected,
			observation.record,
			observation.exists,
		) {
			continue
		}

		if err := s.applyRecordToReplica(
			ctx,
			observation.node,
			key,
			selected,
		); err != nil {
			repairErrors = append(
				repairErrors,
				fmt.Errorf(
					"repair key %d on node %s: %w",
					key,
					observation.node.ID,
					err,
				),
			)
		}
	}

	return repairErrors
}
