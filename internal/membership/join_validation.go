package membership

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// ValidateJoinCandidate returns the one joining node when candidate
// is the next join from current.
func ValidateJoinCandidate(
	current, candidate Configuration,
) (ring.Node, error) {
	if current.identity.Epoch == 0 ||
		current.identity.Epoch == ^uint64(0) ||
		candidate.identity.Epoch != current.identity.Epoch+1 {
		return ring.Node{}, fmt.Errorf("candidate must advance one membership epoch")
	}
	if candidate.virtualNodeCount != current.virtualNodeCount ||
		candidate.replicationFactor != current.replicationFactor {
		return ring.Node{}, fmt.Errorf("candidate changed placement settings")
	}
	if len(candidate.nodes) != len(current.nodes)+1 {
		return ring.Node{}, fmt.Errorf("candidate must add exactly one node")
	}

	oldByID := make(map[string]string, len(current.nodes))
	for _, node := range current.nodes {
		oldByID[node.ID] = node.Address
	}

	var joining ring.Node
	added := 0
	addresses := make(map[string]bool, len(candidate.nodes))

	for _, node := range candidate.nodes {
		if node.ID == "" || node.Address == "" || addresses[node.Address] {
			return ring.Node{}, fmt.Errorf("candidate has invalid or repeated node details")
		}
		addresses[node.Address] = true

		if oldAddress, exists := oldByID[node.ID]; exists {
			if node.Address != oldAddress {
				return ring.Node{}, fmt.Errorf(
					"address changed for existing node %s", node.ID,
				)
			}
			delete(oldByID, node.ID)
			continue
		}

		joining = node
		added++
	}

	if len(oldByID) != 0 || added != 1 {
		return ring.Node{}, fmt.Errorf("candidate did not preserve all old nodes")
	}
	return joining, nil
}

// LoadJoinCandidate checks both the file contents and their relationship
// to the current configuration.
func LoadJoinCandidate(
	path string,
	current Configuration,
) (Configuration, ring.Node, error) {
	candidate, err := LoadCandidate(path)
	if err != nil {
		return Configuration{}, ring.Node{}, err
	}
	joining, err := ValidateJoinCandidate(current, candidate)
	if err != nil {
		return Configuration{}, ring.Node{}, err
	}
	return candidate, joining, nil
}
