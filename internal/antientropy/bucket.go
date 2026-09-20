package antientropy

import (
	"fmt"
	"sort"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

// KeyState contains every known sibling version for one key.
type KeyState struct {
	Key     int64
	Records []store.Record
}

// BucketSnapshot returns a stable, independent copy of all key states
// that belong to the requested Merkle bucket.
func BucketSnapshot(
	snapshot map[int64][]store.Record,
	bucket int,
) ([]KeyState, error) {
	if bucket < 0 || bucket >= BucketCount {
		return nil, fmt.Errorf(
			"bucket %d is outside valid range [0, %d)",
			bucket,
			BucketCount,
		)
	}

	keys := make([]int64, 0)

	for key := range snapshot {
		if BucketForKey(key) == bucket {
			keys = append(keys, key)
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	states := make([]KeyState, 0, len(keys))

	for _, key := range keys {
		records := snapshot[key]
		copiedRecords := make([]store.Record, len(records))

		for i, record := range records {
			copiedRecords[i] = store.Record{
				Value:   record.Value,
				Clock:   cloneClock(record.Clock),
				Deleted: record.Deleted,
			}
		}

		states = append(states, KeyState{
			Key:     key,
			Records: copiedRecords,
		})
	}

	return states, nil
}

// cloneClock creates an independent copy of a vector clock.
func cloneClock(clock version.Clock) version.Clock {
	if clock == nil {
		return nil
	}

	copied := make(version.Clock, len(clock))

	for nodeID, counter := range clock {
		copied[nodeID] = counter
	}

	return copied
}
