package server

import (
	"context"
	"fmt"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// missingReplicaVersions returns global versions that are not already
// represented by the replica's local version set.
func missingReplicaVersions(
	global []store.Record,
	local []store.Record,
) ([]store.Record, error) {
	missing := make([]store.Record, 0)

	for _, wanted := range global {
		covered := false

		for _, existing := range local {
			switch version.Compare(
				existing.Clock,
				wanted.Clock,
			) {
			case version.Equal:
				if existing.Value != wanted.Value ||
					existing.Deleted != wanted.Deleted {
					return nil, status.Error(
						codes.Aborted,
						"equal vector clocks have different contents",
					)
				}

				covered = true

			case version.After:
				// The replica already contains a causally newer version.
				covered = true
			}

			if covered {
				break
			}
		}

		if covered {
			continue
		}

		copied := wanted
		if wanted.Clock != nil {
			copied.Clock = version.Clone(wanted.Clock)
		}

		missing = append(missing, copied)
	}

	return missing, nil
}

// repairObservedSiblingReplicas delivers missing versions to replicas
// that successfully responded to the read.
func (s *GRPCServer) repairObservedSiblingReplicas(
	ctx context.Context,
	key int64,
	versions []store.Record,
	observations []replicaObservation,
) []error {
	var repairErrors []error

	for _, observation := range observations {
		if err := ctx.Err(); err != nil {
			repairErrors = append(repairErrors, err)
			return repairErrors
		}

		local := observation.records

		// Support existing single-record observation callers.
		if len(local) == 0 && observation.exists {
			local = []store.Record{observation.record}
		}

		missing, err := missingReplicaVersions(versions, local)
		if err != nil {
			repairErrors = append(
				repairErrors,
				fmt.Errorf(
					"plan repair for key %d on node %s: %w",
					key,
					observation.node.ID,
					err,
				),
			)
			continue
		}

		for _, record := range missing {
			// Legacy unversioned records cannot use ApplyRecord.
			if len(record.Clock) == 0 {
				continue
			}

			if err := ctx.Err(); err != nil {
				repairErrors = append(repairErrors, err)
				return repairErrors
			}

			if err := s.applyRecordToReplica(
				ctx,
				observation.node,
				key,
				record,
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
	}

	return repairErrors
}
