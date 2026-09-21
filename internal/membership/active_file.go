package membership

import (
	"errors"
	"fmt"
	"os"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// LoadOrBootstrapActive loads the durable active configuration and checks
// that the server's startup ring and replication factor match it.
func LoadOrBootstrapActive(
	path string,
	clusterRing *ring.Ring,
	replicationFactor int,
	bootstrap bool,
) (Configuration, error) {
	if path == "" {
		return Configuration{}, fmt.Errorf("active membership path is required")
	}

	active, err := LoadCandidate(path)
	if errors.Is(err, os.ErrNotExist) {
		if !bootstrap {
			return Configuration{}, fmt.Errorf(
				"active membership file %s is missing; bootstrap it explicitly: %w",
				path, err,
			)
		}

		initial, err := NewConfiguration(1, clusterRing, replicationFactor)
		if err != nil {
			return Configuration{}, err
		}
		if err := SaveCandidate(path, initial); err != nil {
			return Configuration{}, fmt.Errorf("save active membership: %w", err)
		}

		return LoadOrBootstrapActive(path, clusterRing, replicationFactor, false)
	}
	if err != nil {
		return Configuration{}, fmt.Errorf("load active membership: %w", err)
	}

	expected, err := NewConfiguration(
		active.Identity().Epoch,
		clusterRing,
		replicationFactor,
	)
	if err != nil {
		return Configuration{}, err
	}
	if active.Identity() != expected.Identity() {
		return Configuration{}, fmt.Errorf(
			"startup ring or replication factor differs from active membership",
		)
	}

	return active, nil
}
