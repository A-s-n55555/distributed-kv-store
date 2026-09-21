package membership

import (
	"fmt"
	"sort"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
)

type LocalTransfer struct {
	Key          int64
	Destinations []ring.Node
}

// PlanLocalTransfers previews copies needed from one node's snapshot.
// It does not establish that this snapshot has every version of a key.
func PlanLocalTransfers(
	snapshot map[int64][]store.Record,
	current, proposed *ring.Ring,
	sourceID string,
	replicationFactor int,
) ([]LocalTransfer, error) {
	// Validate the rings even when the snapshot is empty.
	if _, err := ring.DiffReplicasForKey(
		current, proposed, 0, replicationFactor,
	); err != nil {
		return nil, err
	}

	_, nodes := current.Configuration()
	sourceExists := false
	for _, node := range nodes {
		if node.ID == sourceID {
			sourceExists = true
			break
		}
	}
	if !sourceExists {
		return nil, fmt.Errorf("source node %q is not in the current ring", sourceID)
	}

	var transfers []LocalTransfer
	for key, records := range snapshot {
		// An empty version set represents no stored key. Tombstones count.
		if len(records) == 0 {
			continue
		}

		diff, err := ring.DiffReplicasForKey(
			current, proposed, key, replicationFactor,
		)
		if err != nil {
			return nil, err
		}

		isOldOwner := false
		for _, node := range diff.Old {
			if node.ID == sourceID {
				isOldOwner = true
				break
			}
		}
		if isOldOwner && len(diff.Added) > 0 {
			transfers = append(transfers, LocalTransfer{
				Key:          key,
				Destinations: diff.Added,
			})
		}
	}

	sort.Slice(transfers, func(i, j int) bool {
		return transfers[i].Key < transfers[j].Key
	})
	return transfers, nil
}
