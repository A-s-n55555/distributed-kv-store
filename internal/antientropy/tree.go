package antientropy

import (
	"crypto/sha256"
	"sort"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	// "github.com/A-s-n55555/distributed-kv-store/internal/version"
)

const BucketCount = 256

// Tree stores a complete binary tree:
// root at index 1, leaves at indices 256 through 511.
type Tree struct {
	nodes [2 * BucketCount]Digest
}

// BucketForKey assigns a key to a stable comparison bucket.
// This is independent of the consistent-hash ownership ring.
func BucketForKey(key int64) int {
	h := sha256.New()
	writeString(h, "kv-bucket-assignment-v1")
	writeUint64(h, uint64(key))
	digest := finishDigest(h)

	return int(digest[0])
}

// BuildTree hashes an independent snapshot.
// The caller must not mutate the snapshot during this call.
func BuildTree(snapshot map[int64][]store.Record) *Tree {
	var buckets [BucketCount][]int64

	for key, records := range snapshot {
		// An empty version set represents absence.
		if len(records) == 0 {
			continue
		}

		bucket := BucketForKey(key)
		buckets[bucket] = append(buckets[bucket], key)
	}

	tree := &Tree{}

	for bucket := 0; bucket < BucketCount; bucket++ {
		keys := buckets[bucket]

		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})

		h := sha256.New()
		writeString(h, "kv-merkle-leaf-v1")
		writeUint64(h, uint64(bucket))
		writeUint64(h, uint64(len(keys)))

		for _, key := range keys {
			digest := KeyDigest(key, snapshot[key])
			_, _ = h.Write(digest[:])
		}

		tree.nodes[BucketCount+bucket] = finishDigest(h)
	}

	for index := BucketCount - 1; index >= 1; index-- {
		h := sha256.New()
		writeString(h, "kv-merkle-branch-v1")

		left := tree.nodes[index*2]
		right := tree.nodes[index*2+1]

		_, _ = h.Write(left[:])
		_, _ = h.Write(right[:])

		tree.nodes[index] = finishDigest(h)
	}

	return tree
}

// Root returns the fingerprint for the complete snapshot.
func (t *Tree) Root() Digest {
	return t.nodes[1]
}

// DifferentBuckets returns differing bucket IDs in ascending order.
// Both trees must have been constructed with BuildTree.
func DifferentBuckets(first, second *Tree) []int {
	var different []int

	var visit func(index int)
	visit = func(index int) {
		if first.nodes[index] == second.nodes[index] {
			return
		}

		if index >= BucketCount {
			different = append(different, index-BucketCount)
			return
		}

		visit(index * 2)
		visit(index*2 + 1)
	}

	visit(1)
	return different
}

// BucketDigests returns an independent copy of the Merkle bucket digests.
// The slice index represents the bucket number.
// BucketDigests returns an independent copy of all bucket digests.
// BucketDigests returns an independent copy of all Merkle leaf digests.
// The returned slice index represents the bucket number.
func (t *Tree) BucketDigests() []Digest {
	digests := make([]Digest, BucketCount)
	copy(digests, t.nodes[BucketCount:])
	return digests
}
