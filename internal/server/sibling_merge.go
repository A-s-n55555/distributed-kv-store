package server

import (
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mergeReplicaVersions returns distinct, non-dominated records.
// Equal clocks with different contents are invalid, not valid siblings.
func mergeReplicaVersions(
	records []store.Record,
) ([]store.Record, error) {
	// Check contradictions before pruning superseded records.
	for i := range records {
		for j := i + 1; j < len(records); j++ {
			if version.Compare(
				records[i].Clock,
				records[j].Clock,
			) != version.Equal {
				continue
			}

			if records[i].Value != records[j].Value ||
				records[i].Deleted != records[j].Deleted {
				return nil, status.Error(
					codes.Aborted,
					"equal vector clocks have different contents",
				)
			}
		}
	}

	var result []store.Record

	for i, record := range records {
		discard := false

		for j, other := range records {
			if i == j {
				continue
			}

			relation := version.Compare(record.Clock, other.Clock)

			if relation == version.Before ||
				(relation == version.Equal && j < i) {
				discard = true
				break
			}
		}

		if discard {
			continue
		}

		// Never expose a replica response's clock map directly.
		copied := record
		if record.Clock != nil {
			copied.Clock = version.Clone(record.Clock)
		}

		result = append(result, copied)
	}

	return result, nil
}
