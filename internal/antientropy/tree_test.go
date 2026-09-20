package antientropy

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestTreeIgnoresMapAndSiblingOrder(t *testing.T) {
	first := store.Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}
	second := store.Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}
	deleted := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 2},
	}

	left := make(map[int64][]store.Record)
	left[10] = []store.Record{first, second}
	left[20] = []store.Record{deleted}

	right := make(map[int64][]store.Record)
	right[20] = []store.Record{deleted}
	right[10] = []store.Record{second, first}

	a := BuildTree(left)
	b := BuildTree(right)

	if a.Root() != b.Root() {
		t.Fatal("ordering changed the root")
	}

	if got := DifferentBuckets(a, b); len(got) != 0 {
		t.Fatalf("identical states differ in buckets %v", got)
	}
}

func TestTreeFindsChangedKeyBucket(t *testing.T) {
	before := map[int64][]store.Record{
		10: {
			{
				Value: "old",
				Clock: version.Clock{"node-1": 1},
			},
		},
	}

	after := map[int64][]store.Record{
		10: {
			{
				Value: "new",
				Clock: version.Clock{"node-1": 2},
			},
		},
	}

	got := DifferentBuckets(BuildTree(before), BuildTree(after))
	want := BucketForKey(10)

	if len(got) != 1 || got[0] != want {
		t.Fatalf("different buckets = %v; want [%d]", got, want)
	}
}

func TestTreeDetectsMissingSiblingAndTombstone(t *testing.T) {
	first := store.Record{
		Value: "live",
		Clock: version.Clock{"node-1": 1},
	}
	tombstone := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}

	complete := BuildTree(map[int64][]store.Record{
		10: {first, tombstone},
	})
	incomplete := BuildTree(map[int64][]store.Record{
		10: {first},
	})

	got := DifferentBuckets(complete, incomplete)
	if len(got) != 1 || got[0] != BucketForKey(10) {
		t.Fatalf("missing tombstone sibling not located: %v", got)
	}

	deleted := BuildTree(map[int64][]store.Record{
		10: {tombstone},
	})
	empty := BuildTree(nil)

	if deleted.Root() == empty.Root() {
		t.Fatal("tombstone was treated as absence")
	}
}

func TestTreeLocatesMultipleBuckets(t *testing.T) {
	firstKey := int64(1)
	secondKey := int64(2)

	// Find another key assigned to a different bucket.
	for BucketForKey(secondKey) == BucketForKey(firstKey) {
		secondKey++
	}

	record := store.Record{
		Value: "value",
		Clock: version.Clock{"node-1": 1},
	}

	populated := BuildTree(map[int64][]store.Record{
		firstKey:  {record},
		secondKey: {record},
	})

	got := DifferentBuckets(BuildTree(nil), populated)

	firstBucket := BucketForKey(firstKey)
	secondBucket := BucketForKey(secondKey)
	if firstBucket > secondBucket {
		firstBucket, secondBucket = secondBucket, firstBucket
	}

	if len(got) != 2 ||
		got[0] != firstBucket ||
		got[1] != secondBucket {
		t.Fatalf(
			"different buckets = %v; want [%d %d]",
			got,
			firstBucket,
			secondBucket,
		)
	}
}

func TestTreeNormalizesEmptyVersionSet(t *testing.T) {
	empty := BuildTree(nil)
	emptyEntry := BuildTree(map[int64][]store.Record{
		99: {},
	})

	if empty.Root() != emptyEntry.Root() {
		t.Fatal("empty version set should represent absence")
	}
}
func TestBucketDigestsReturnsIndependentCopy(t *testing.T) {
	tree := BuildTree(map[int64][]store.Record{
		1: {
			{
				Value: "one",
				Clock: version.Clock{"node-1": 1},
			},
		},
	})

	first := tree.BucketDigests()

	if len(first) != BucketCount {
		t.Fatalf(
			"BucketDigests returned %d digests; want %d",
			len(first),
			BucketCount,
		)
	}

	bucket := BucketForKey(1)
	originalByte := first[bucket][0]

	// Mutate the returned copy.
	first[bucket][0] ^= 0xff

	second := tree.BucketDigests()

	if second[bucket][0] != originalByte {
		t.Fatal("BucketDigests exposed the tree's internal digest storage")
	}
}
