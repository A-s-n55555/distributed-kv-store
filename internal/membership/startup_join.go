package membership

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// LoadJoinAwareActive accepts the configured ring when it describes either
// the active candidate or the saved old membership of an activated join.
func LoadJoinAwareActive(
	activePath, pausePath string,
	configuredRing *ring.Ring,
	replicationFactor int,
	bootstrap bool,
) (Configuration, error) {
	active, err := LoadOrBootstrapActive(
		activePath, configuredRing, replicationFactor, bootstrap,
	)
	if err == nil {
		return active, nil
	}

	activated, loadErr := LoadCandidate(activePath)
	if loadErr != nil {
		return Configuration{}, err
	}
	previous, recoveryErr := LoadActivatedJoin(pausePath, activated)
	if recoveryErr != nil {
		return Configuration{}, fmt.Errorf(
			"active membership does not match startup ring; "+
				"activated join recovery: %w", recoveryErr,
		)
	}

	configured, configErr := NewConfiguration(
		previous.Identity().Epoch, configuredRing, replicationFactor,
	)
	if configErr != nil || configured.Identity() != previous.Identity() {
		return Configuration{}, fmt.Errorf(
			"startup ring matches neither active membership nor " +
				"the saved previous membership",
		)
	}
	return activated, nil
}
