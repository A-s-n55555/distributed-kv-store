package antientropy

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"sort"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
)

// Digest is a fixed-size fingerprint.
type Digest [sha256.Size]byte

// RecordDigest fingerprints a record's value, deletion flag, and clock.
// Nil and empty clocks have the same representation.
func RecordDigest(record store.Record) Digest {
	h := sha256.New()

	// Encoding version and domain separate record hashes from key hashes.
	writeString(h, "kv-record-v1")
	writeString(h, record.Value)

	var deleted uint64
	if record.Deleted {
		deleted = 1
	}
	writeUint64(h, deleted)

	nodeIDs := make([]string, 0, len(record.Clock))
	for nodeID := range record.Clock {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	writeUint64(h, uint64(len(nodeIDs)))

	for _, nodeID := range nodeIDs {
		writeString(h, nodeID)
		writeUint64(h, record.Clock[nodeID])
	}

	return finishDigest(h)
}

// KeyDigest fingerprints a key and its retained version set.
// Sibling order and identical duplicate records do not affect the result.
// Callers should supply the store's non-dominated versions.
func KeyDigest(key int64, records []store.Record) Digest {
	unique := make(map[Digest]struct{}, len(records))
	digests := make([]Digest, 0, len(records))

	for _, record := range records {
		digest := RecordDigest(record)

		if _, exists := unique[digest]; exists {
			continue
		}

		unique[digest] = struct{}{}
		digests = append(digests, digest)
	}

	sort.Slice(digests, func(i, j int) bool {
		return bytes.Compare(digests[i][:], digests[j][:]) < 0
	})

	h := sha256.New()
	writeString(h, "kv-key-v1")

	// Conversion preserves a distinct encoding for every int64 key.
	writeUint64(h, uint64(key))
	writeUint64(h, uint64(len(digests)))

	for _, digest := range digests {
		_, _ = h.Write(digest[:])
	}

	return finishDigest(h)
}

func writeUint64(h hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = h.Write(encoded[:])
}

// Length prefixes prevent ambiguous concatenation of strings.
func writeString(h hash.Hash, value string) {
	writeUint64(h, uint64(len(value)))
	_, _ = h.Write([]byte(value))
}

func finishDigest(h hash.Hash) Digest {
	var digest Digest
	copy(digest[:], h.Sum(nil))
	return digest
}
