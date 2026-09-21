package membership

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// JoinCoordinator selects the decision owner from the active membership.
func JoinCoordinator(current Configuration) (ring.Node, error) {
	nodes := current.Nodes()
	if current.Identity().Epoch == 0 || len(nodes) == 0 {
		return ring.Node{}, fmt.Errorf("active membership is required")
	}

	coordinator := nodes[0]
	for _, node := range nodes[1:] {
		if node.ID < coordinator.ID {
			coordinator = node
		}
	}
	return coordinator, nil
}
