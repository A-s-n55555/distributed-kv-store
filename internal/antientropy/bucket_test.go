package antientropy

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestBucketSnapshotFiltersSortsAndCopies(t *testing.T) {
	firstKey, secondKey, otherKey := findBucketTestKeys(t)

	targetBucket := BucketForKey(firstKey)

	snapshot := map[int64][]store.Record{
		secondKey: {
			{
				Value: "second",
				Clock: version.Clock{"node-2": 2},
			},
		},
		firstKey: {
			{
				Value: "first-a",
				Clock: version.Clock{"node-1": 1},
			},
			{
				Value: "first-b",
				Clock: version.Clock{"node-2": 1},
			},
		},
		otherKey: {
			{
				Value: "other",
				Clock: version.Clock{"node-1": 3},
			},
		},
	}

	states, err := BucketSnapshot(snapshot, targetBucket)
	if err != nil {
		t.Fatal(err)
	}

	if len(states) != 2 {
		t.Fatalf("got %d key states; want 2", len(states))
	}

	if states[0].Key != firstKey || states[1].Key != secondKey {
		t.Fatalf(
			"keys = [%d, %d]; want [%d, %d]",
			states[0].Key,
			states[1].Key,
			firstKey,
			secondKey,
		)
	}

	if len(states[0].Records) != 2 {
		t.Fatalf(
			"first key has %d records; want 2",
			len(states[0].Records),
		)
	}

	// Verify the returned clocks do not alias the original snapshot.
	states[0].Records[0].Clock["node-1"] = 99

	if snapshot[firstKey][0].Clock["node-1"] != 1 {
		t.Fatal("BucketSnapshot returned an aliased vector clock")
	}
}

func TestBucketSnapshotRejectsInvalidBucket(t *testing.T) {
	tests := []int{-1, BucketCount}

	for _, bucket := range tests {
		if _, err := BucketSnapshot(nil, bucket); err == nil {
			t.Fatalf("BucketSnapshot accepted invalid bucket %d", bucket)
		}
	}
}

func findBucketTestKeys(t *testing.T) (int64, int64, int64) {
	t.Helper()

	var firstKey int64
	var secondKey int64
	var otherKey int64

	targetBucket := -1

	for key := int64(0); key < 1_000_000; key++ {
		bucket := BucketForKey(key)

		if targetBucket == -1 {
			firstKey = key
			targetBucket = bucket
			continue
		}

		if bucket == targetBucket && key != firstKey {
			secondKey = key
		}

		if bucket != targetBucket {
			otherKey = key
		}

		if secondKey != 0 && otherKey != 0 {
			return firstKey, secondKey, otherKey
		}
	}

	t.Fatal("could not find suitable bucket test keys")
	return 0, 0, 0
}