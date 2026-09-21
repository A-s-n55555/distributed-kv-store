package membership

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// Identity distinguishes a membership epoch and its placement settings.
type Identity struct {
	Epoch  uint64
	Digest antientropy.Digest
}

// Configuration is an independent snapshot of proposed or active membership.
type Configuration struct {
	identity          Identity
	virtualNodeCount  int
	replicationFactor int
	nodes             []ring.Node
}

func NewConfiguration(
	epoch uint64,
	clusterRing *ring.Ring,
	replicationFactor int,
) (Configuration, error) {
	if epoch == 0 {
		return Configuration{}, fmt.Errorf("membership epoch must be positive")
	}
	if clusterRing == nil {
		return Configuration{}, fmt.Errorf("cluster ring is required")
	}

	virtualNodes, nodes := clusterRing.Configuration()
	digest, err := antientropy.ConfigurationDigest(
		virtualNodes, replicationFactor, nodes,
	)
	if err != nil {
		return Configuration{}, err
	}

	return Configuration{
		identity: Identity{
			Epoch:  epoch,
			Digest: digest,
		},
		virtualNodeCount:  virtualNodes,
		replicationFactor: replicationFactor,
		nodes:             append([]ring.Node(nil), nodes...),
	}, nil
}

func (c Configuration) Identity() Identity {
	return c.identity
}

func (c Configuration) Nodes() []ring.Node {
	return append([]ring.Node(nil), c.nodes...)
}

func (c Configuration) VirtualNodeCount() int {
	return c.virtualNodeCount
}

func (c Configuration) ReplicationFactor() int {
	return c.replicationFactor
}
