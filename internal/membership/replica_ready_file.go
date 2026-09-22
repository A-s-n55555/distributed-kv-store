package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func joinReplicaReadyPath(pausePath string) string {
	return filepath.Join(
		filepath.Dir(pausePath),
		"membership-replica-ready.json",
	)
}

// validateReplicaReadyMembership requires a fully installed activation.
func validateReplicaReadyMembership(
	pausePath string,
	active Configuration,
) error {
	if pausePath == "" {
		return fmt.Errorf("join pause path is required")
	}
	if _, err := LoadActivatedJoin(pausePath, active); err != nil {
		return fmt.Errorf("validate activated join: %w", err)
	}

	saved, err := LoadCandidate(filepath.Join(
		filepath.Dir(pausePath), "membership-active.json",
	))
	if err != nil {
		return fmt.Errorf("load active membership: %w", err)
	}
	if saved.Identity() != active.Identity() {
		return fmt.Errorf("active file differs from replica-ready membership")
	}
	return nil
}

// LoadJoinReplicaReady returns false, nil only when its marker is absent.
// A present but invalid marker returns an error.
func LoadJoinReplicaReady(
	pausePath string,
	active Configuration,
) (bool, error) {
	if pausePath == "" {
		return false, fmt.Errorf("join pause path is required")
	}

	saved, err := LoadCandidate(joinReplicaReadyPath(pausePath))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load replica-ready marker: %w", err)
	}
	if saved.Identity() != active.Identity() {
		return false, fmt.Errorf("replica-ready marker belongs to another membership")
	}
	if err := validateReplicaReadyMembership(pausePath, active); err != nil {
		return false, err
	}
	return true, nil
}

// RecordJoinReplicaReady persists local authorization to accept replicas.
// Caller must first verify the coordinator's resume decision and serialize
// this operation. This function does not change any in-memory gates.
func RecordJoinReplicaReady(
	pausePath string,
	active Configuration,
) error {
	if err := validateReplicaReadyMembership(pausePath, active); err != nil {
		return err
	}
	if _, err := LoadJoinReplicaReady(pausePath, active); err != nil {
		return err
	}

	// Same-candidate retries repeat the durable write safely.
	if err := SaveCandidate(joinReplicaReadyPath(pausePath), active); err != nil {
		return fmt.Errorf("persist replica readiness: %w", err)
	}
	return nil
}
