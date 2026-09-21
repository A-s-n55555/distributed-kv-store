package membership

import (
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// ValidateJoinIntent rebuilds a proposed join from local active membership.
// A peer must match both the active and resulting candidate identities.
func ValidateJoinIntent(
	current Configuration,
	expectedActive Identity,
	joining ring.Node,
	expectedCandidate Identity,
) (Configuration, error) {
	if current.Identity() != expectedActive {
		return Configuration{}, fmt.Errorf("active membership identity does not match")
	}

	candidate, err := PrepareJoin(current, joining)
	if err != nil {
		return Configuration{}, fmt.Errorf("prepare requested join: %w", err)
	}
	if candidate.Identity() != expectedCandidate {
		return Configuration{}, fmt.Errorf("join candidate identity does not match")
	}

	return candidate, nil
}
