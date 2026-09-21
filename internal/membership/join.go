package membership

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// Ring builds an independent ring from this configuration.
func (c Configuration) Ring() *ring.Ring {
	result := ring.New(c.virtualNodeCount)
	for _, node := range c.nodes {
		result.AddNode(node)
	}
	return result
}

// PrepareJoin creates the next configuration without changing current.
func PrepareJoin(
	current Configuration,
	joining ring.Node,
) (Configuration, error) {
	if current.identity.Epoch == 0 || current.virtualNodeCount <= 0 {
		return Configuration{}, fmt.Errorf("current configuration is invalid")
	}
	if current.identity.Epoch == ^uint64(0) {
		return Configuration{}, fmt.Errorf("membership epoch has overflowed")
	}
	if joining.ID == "" || joining.Address == "" {
		return Configuration{}, fmt.Errorf("joining node needs an ID and address")
	}

	for _, existing := range current.nodes {
		if existing.ID == joining.ID {
			return Configuration{}, fmt.Errorf(
				"node ID %q already exists", joining.ID,
			)
		}
		if existing.Address == joining.Address {
			return Configuration{}, fmt.Errorf(
				"address %q already belongs to node %s",
				joining.Address, existing.ID,
			)
		}
	}

	candidate := current.Ring()
	candidate.AddNode(joining)

	return NewConfiguration(
		current.identity.Epoch+1,
		candidate,
		current.replicationFactor,
	)
}
