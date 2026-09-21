package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func joinAbortPath(pausePath string, identity Identity) (string, error) {
	if pausePath == "" || identity.Epoch == 0 {
		return "", fmt.Errorf("pause path and candidate epoch are required")
	}

	name := fmt.Sprintf(
		"membership-aborted-%d-%x.json",
		identity.Epoch,
		identity.Digest[:],
	)
	return filepath.Join(filepath.Dir(pausePath), name), nil
}

// RecordJoinAbort durably records an abort for one validated candidate.
// An existing corrupt marker is an error; it is never overwritten.
func RecordJoinAbort(
	pausePath string,
	current, candidate Configuration,
) error {
	if _, err := ValidateJoinCandidate(current, candidate); err != nil {
		return err
	}

	path, err := joinAbortPath(pausePath, candidate.Identity())
	if err != nil {
		return err
	}

	existing, _, err := LoadJoinCandidate(path, current)
	if err == nil {
		if existing.Identity() != candidate.Identity() {
			return fmt.Errorf("abort marker belongs to another candidate")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect abort marker: %w", err)
	}

	return SaveCandidate(path, candidate)
}

// JoinWasAborted checks a candidate's durable abort marker.
func JoinWasAborted(
	pausePath string,
	current, candidate Configuration,
) (bool, error) {
	if _, err := ValidateJoinCandidate(current, candidate); err != nil {
		return false, err
	}

	path, err := joinAbortPath(pausePath, candidate.Identity())
	if err != nil {
		return false, err
	}

	saved, _, err := LoadJoinCandidate(path, current)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load abort marker: %w", err)
	}
	if saved.Identity() != candidate.Identity() {
		return false, fmt.Errorf("abort marker belongs to another candidate")
	}
	return true, nil
}
