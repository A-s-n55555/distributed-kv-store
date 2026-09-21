package ring

import (
	"fmt"
	"strconv"
)

// ReplicaDiff describes a proposed placement change for one key.
// Added nodes need the key; Removed nodes must retain it until transfer
// and activation have been completed safely.
type ReplicaDiff struct {
	Old     []Node
	New     []Node
	Added   []Node
	Removed []Node
}

func DiffReplicasForKey(
	oldRing, newRing *Ring,
	key int64,
	replicationFactor int,
) (ReplicaDiff, error) {
	if oldRing == nil || newRing == nil {
		return ReplicaDiff{}, fmt.Errorf("both rings are required")
	}
	if replicationFactor <= 0 {
		return ReplicaDiff{}, fmt.Errorf("replication factor must be positive")
	}

	oldVirtualNodes, oldNodes := oldRing.Configuration()
	newVirtualNodes, newNodes := newRing.Configuration()

	if oldVirtualNodes <= 0 || oldVirtualNodes != newVirtualNodes {
		return ReplicaDiff{}, fmt.Errorf("virtual-node counts must match and be positive")
	}
	if len(oldNodes) < replicationFactor || len(newNodes) < replicationFactor {
		return ReplicaDiff{}, fmt.Errorf("both rings need at least %d physical nodes",
			replicationFactor)
	}

	oldAddresses := make(map[string]string, len(oldNodes))
	for _, node := range oldNodes {
		oldAddresses[node.ID] = node.Address
	}
	for _, node := range newNodes {
		if address, exists := oldAddresses[node.ID]; exists &&
			address != node.Address {
			return ReplicaDiff{}, fmt.Errorf("address changed for node %q", node.ID)
		}
	}

	ringKey := strconv.FormatInt(key, 10)
	diff := ReplicaDiff{
		Old: oldRing.GetReplicas(ringKey, replicationFactor),
		New: newRing.GetReplicas(ringKey, replicationFactor),
	}
	if len(diff.Old) != replicationFactor ||
		len(diff.New) != replicationFactor {
		return ReplicaDiff{}, fmt.Errorf("could not select %d replicas",
			replicationFactor)
	}

	oldIDs := make(map[string]bool, len(diff.Old))
	newIDs := make(map[string]bool, len(diff.New))
	for _, node := range diff.Old {
		oldIDs[node.ID] = true
	}
	for _, node := range diff.New {
		newIDs[node.ID] = true
		if !oldIDs[node.ID] {
			diff.Added = append(diff.Added, node)
		}
	}
	for _, node := range diff.Old {
		if !newIDs[node.ID] {
			diff.Removed = append(diff.Removed, node)
		}
	}
	return diff, nil
}
