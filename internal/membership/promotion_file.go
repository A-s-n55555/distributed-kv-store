package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func stagingPromotionPath(pausePath string) string {
	return filepath.Join(
		filepath.Dir(pausePath),
		"membership-promotion.json",
	)
}

// LoadStagingPromotion loads a durable promotion decision for the joining node.
// A missing promotion marker returns an error wrapping os.ErrNotExist.
func LoadStagingPromotion(
	pausePath, nodeID string,
) (Configuration, Configuration, error) {
	if pausePath == "" || nodeID == "" {
		return Configuration{}, Configuration{},
			fmt.Errorf("pause path and node ID are required")
	}

	candidate, err := LoadCandidate(stagingPromotionPath(pausePath))
	if err != nil {
		return Configuration{}, Configuration{},
			fmt.Errorf("load promotion marker: %w", err)
	}

	previous, err := LoadPreviousJoin(pausePath)
	if err != nil {
		// A promotion marker without its previous configuration is invalid.
		// Do not expose this as an absent promotion.
		return Configuration{}, Configuration{},
			fmt.Errorf("promotion has invalid previous membership: %v", err)
	}

	joining, err := ValidateJoinCandidate(previous, candidate)
	if err != nil {
		return Configuration{}, Configuration{},
			fmt.Errorf("invalid saved promotion: %w", err)
	}
	if joining.ID != nodeID {
		return Configuration{}, Configuration{},
			fmt.Errorf("promotion belongs to joining node %s", joining.ID)
	}

	return previous, candidate, nil
}

// RecordStagingPromotion records an irreversible promotion decision.
// The caller must first authorize the request, verify old-member activation,
// and close/drain the staging node's request gates.
//
// Calls must be serialized by the server. The parent directory must exist.
func RecordStagingPromotion(
	pausePath, nodeID string,
	current, candidate Configuration,
) error {
	if pausePath == "" || nodeID == "" {
		return fmt.Errorf("pause path and node ID are required")
	}

	joining, err := ValidateJoinCandidate(current, candidate)
	if err != nil {
		return fmt.Errorf("invalid promotion candidate: %w", err)
	}
	if joining.ID != nodeID {
		return fmt.Errorf("only joining node %s can be promoted", joining.ID)
	}

	savedPrevious, savedCandidate, err := LoadStagingPromotion(
		pausePath, nodeID,
	)
	switch {
	case err == nil:
		if savedPrevious.Identity() != current.Identity() ||
			savedCandidate.Identity() != candidate.Identity() {
			return fmt.Errorf("another promotion is already recorded")
		}
		// Re-persist on retry, including directory synchronization.
		return SaveCandidate(stagingPromotionPath(pausePath), candidate)

	case !errors.Is(err, os.ErrNotExist):
		return err
	}

	// Save the old configuration before the decision marker.
	previous, err := LoadPreviousJoin(pausePath)
	switch {
	case err == nil:
		if previous.Identity() != current.Identity() {
			return fmt.Errorf("saved previous membership belongs to another join")
		}
	case errors.Is(err, os.ErrNotExist):
		// No previous snapshot yet.
	default:
		return fmt.Errorf("inspect previous membership: %w", err)
	}

	if err := SaveCandidate(previousJoinPath(pausePath), current); err != nil {
		return fmt.Errorf("save promotion previous membership: %w", err)
	}
	if err := SaveCandidate(
		stagingPromotionPath(pausePath), candidate,
	); err != nil {
		return fmt.Errorf("save promotion decision: %w", err)
	}
	return nil
}
