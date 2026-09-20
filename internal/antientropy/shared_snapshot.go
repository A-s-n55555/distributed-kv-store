package antientropy

import (
	"fmt"
	"strconv"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

// SharedSnapshot selects locally present keys assigned to both nodes.
// It returns independent copies, including siblings and tombstones.
//
// Both peers must use the same membership and replication factor.
func SharedSnapshot(
	snapshot map[int64][]store.Record,
	clusterRing *ring.Ring,
	replicationFactor int,
	localID string,
	peerID string,
) (map[int64][]store.Record, error) {
	if clusterRing == nil {
		return nil, fmt.Errorf("cluster ring is required")
	}
	if replicationFactor <= 0 {
		return nil, fmt.Errorf("replication factor must be positive")
	}
	if localID == "" || peerID == "" {
		return nil, fmt.Errorf("both node IDs are required")
	}
	if localID == peerID {
		return nil, fmt.Errorf("anti-entropy requires distinct nodes")
	}

	// Verify that the ring can satisfy the requested replica count,
	// even when the snapshot is empty.
	if len(clusterRing.GetReplicas("0", replicationFactor)) !=
		replicationFactor {
		return nil, fmt.Errorf(
			"cluster cannot satisfy replication factor %d",
			replicationFactor,
		)
	}

	shared := make(map[int64][]store.Record)

	for key, records := range snapshot {
		replicas := clusterRing.GetReplicas(
			strconv.FormatInt(key, 10),
			replicationFactor,
		)

		if len(replicas) != replicationFactor {
			return nil, fmt.Errorf(
				"incomplete replica placement for key %d",
				key,
			)
		}

		hasLocal := false
		hasPeer := false

		for _, node := range replicas {
			if node.ID == localID {
				hasLocal = true
			}
			if node.ID == peerID {
				hasPeer = true
			}
		}

		if !hasLocal || !hasPeer || len(records) == 0 {
			continue
		}

		copied := make([]store.Record, len(records))
		for i, record := range records {
			copied[i] = record
			if record.Clock != nil {
				copied[i].Clock = version.Clone(record.Clock)
			}
		}

		shared[key] = copied
	}

	return shared, nil
}
