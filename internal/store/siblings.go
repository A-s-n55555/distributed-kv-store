package store

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

// mergeRecordVersions combines versions without discarding concurrent data.
// It does not modify the input records or persist anything.
func mergeRecordVersions(
	existing []Record,
	incoming Record,
) ([]Record, error) {
	all := make([]Record, 0, len(existing)+1)
	all = append(all, existing...)
	all = append(all, incoming)

	// Equal clocks must describe identical contents.
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if version.Compare(all[i].Clock, all[j].Clock) !=
				version.Equal {
				continue
			}

			if all[i].Value != all[j].Value ||
				all[i].Deleted != all[j].Deleted {
				return nil, fmt.Errorf(
					"%w: equal clocks have different contents",
					ErrRecordConflict,
				)
			}
		}
	}

	result := make([]Record, 0, len(all))

	for i, record := range all {
		discard := false

		for j, other := range all {
			if i == j {
				continue
			}

			relation := version.Compare(record.Clock, other.Clock)

			if relation == version.Before {
				discard = true
				break
			}

			// Keep only the first copy of identical versions.
			if relation == version.Equal && j < i {
				discard = true
				break
			}
		}

		if discard {
			continue
		}

		if record.Clock != nil {
			record.Clock = version.Clone(record.Clock)
		}

		result = append(result, record)
	}

	return result, nil
}
