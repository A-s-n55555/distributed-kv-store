package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// loadReleaseMarker validates a present marker against local replica readiness.
func loadReleaseMarker(
	pausePath, filename string,
	active Configuration,
) (bool, error) {
	if pausePath == "" {
		return false, fmt.Errorf("join pause path is required")
	}

	path := filepath.Join(filepath.Dir(pausePath), filename)
	saved, err := LoadCandidate(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load %s: %w", filename, err)
	}
	if saved.Identity() != active.Identity() {
		return false, fmt.Errorf("%s belongs to another membership", filename)
	}

	ready, err := LoadJoinReplicaReady(pausePath, active)
	if err != nil {
		return false, err
	}
	if !ready {
		return false, fmt.Errorf("%s exists without replica readiness", filename)
	}
	return true, nil
}

func LoadJoinClientReleaseDecision(
	pausePath string,
	active Configuration,
) (bool, error) {
	return loadReleaseMarker(
		pausePath, "membership-client-release-decision.json", active,
	)
}

// RecordJoinClientReleaseDecision is coordinator-only.
// Caller must first verify replica readiness on EVERY candidate member.
// Calls must be serialized.
func RecordJoinClientReleaseDecision(
	pausePath, nodeID string,
	active Configuration,
) error {
	previous, err := LoadActivatedJoin(pausePath, active)
	if err != nil {
		return err
	}
	coordinator, err := JoinCoordinator(previous)
	if err != nil {
		return err
	}
	if nodeID != coordinator.ID {
		return fmt.Errorf(
			"only coordinator %s can authorize client release",
			coordinator.ID,
		)
	}

	resumeDecided, err := LoadJoinResumeDecision(pausePath, active)
	if err != nil {
		return err
	}
	if !resumeDecided {
		return fmt.Errorf("resume decision is missing")
	}

	ready, err := LoadJoinReplicaReady(pausePath, active)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("coordinator is not replica-ready")
	}

	if _, err := LoadJoinClientReleaseDecision(pausePath, active); err != nil {
		return err
	}
	return SaveCandidate(
		filepath.Join(
			filepath.Dir(pausePath),
			"membership-client-release-decision.json",
		),
		active,
	)
}

func LoadJoinClientsReleased(
	pausePath string,
	active Configuration,
) (bool, error) {
	return loadReleaseMarker(
		pausePath, "membership-clients-released.json", active,
	)
}

// RecordJoinClientsReleased records local completion.
// Caller must first verify the coordinator's client-release decision.
// Calls must be serialized; this helper does not change in-memory gates.
func RecordJoinClientsReleased(
	pausePath string,
	active Configuration,
) error {
	ready, err := LoadJoinReplicaReady(pausePath, active)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("cannot release clients before replica readiness")
	}
	if _, err := LoadJoinClientsReleased(pausePath, active); err != nil {
		return err
	}
	return SaveCandidate(
		filepath.Join(
			filepath.Dir(pausePath),
			"membership-clients-released.json",
		),
		active,
	)
}
