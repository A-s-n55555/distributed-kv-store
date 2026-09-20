package antientropy

import (
	"crypto/sha256"
	"fmt"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"hash"
	"sort"
)

// ConfigurationDigest fingerprints the placement and anti-entropy
// configuration that two peers must agree on before comparing data.
func ConfigurationDigest(
	virtualNodeCount int,
	replicationFactor int,
	nodes []ring.Node,
) (Digest, error) {
	if virtualNodeCount <= 0 {
		return Digest{}, fmt.Errorf(
			"virtual-node count must be positive",
		)
	}

	if replicationFactor <= 0 {
		return Digest{}, fmt.Errorf(
			"replication factor must be positive",
		)
	}

	if replicationFactor > len(nodes) {
		return Digest{}, fmt.Errorf(
			"replication factor %d exceeds physical node count %d",
			replicationFactor,
			len(nodes),
		)
	}

	sorted := append([]ring.Node(nil), nodes...)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ID == sorted[j].ID {
			return sorted[i].Address < sorted[j].Address
		}

		return sorted[i].ID < sorted[j].ID
	})

	seen := make(map[string]struct{}, len(sorted))

	h := newConfigurationHasher()

	writeString(h, "kv-cluster-configuration-v1")
	writeString(h, "ring-hash-fnv32a-v1")
	writeString(h, "anti-entropy-sha256-v1")
	writeUint64(h, uint64(BucketCount))
	writeUint64(h, uint64(virtualNodeCount))
	writeUint64(h, uint64(replicationFactor))
	writeUint64(h, uint64(len(sorted)))

	for _, node := range sorted {
		if node.ID == "" {
			return Digest{}, fmt.Errorf("node ID is required")
		}

		if node.Address == "" {
			return Digest{}, fmt.Errorf(
				"address is required for node %s",
				node.ID,
			)
		}

		if _, exists := seen[node.ID]; exists {
			return Digest{}, fmt.Errorf(
				"duplicate node ID %q",
				node.ID,
			)
		}
		seen[node.ID] = struct{}{}

		writeString(h, node.ID)
		writeString(h, node.Address)
	}

	return finishDigest(h), nil
}

func newConfigurationHasher() hash.Hash {
	return sha256.New()
}
