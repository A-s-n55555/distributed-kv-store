package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func previousJoinPath(pausePath string) string {
	return filepath.Join(
		filepath.Dir(pausePath),
		"membership-join-previous.json",
	)
}

// ActivateCommittedJoin saves the old configuration, then installs the
// candidate. It never clears the pause or opens request gates.
func ActivateCommittedJoin(
	activePath, pausePath string,
	current, candidate Configuration,
) error {
	if activePath == "" || pausePath == "" {
		return fmt.Errorf("active and join pause paths are required")
	}
	if _, err := ValidateJoinCandidate(current, candidate); err != nil {
		return fmt.Errorf("invalid activation candidate: %w", err)
	}

	paused, err := LoadJoinPause(pausePath, current)
	if err != nil {
		return fmt.Errorf("load join pause: %w", err)
	}
	if paused.Identity() != candidate.Identity() {
		return fmt.Errorf("join pause belongs to another candidate")
	}

	committed, err := LoadJoinCommit(pausePath, current)
	if err != nil {
		return fmt.Errorf("load join commitment: %w", err)
	}
	if committed.Identity() != candidate.Identity() {
		return fmt.Errorf("join commitment belongs to another candidate")
	}

	active, err := LoadCandidate(activePath)
	if err != nil {
		return fmt.Errorf("load active membership: %w", err)
	}
	if active.Identity() != current.Identity() &&
		active.Identity() != candidate.Identity() {
		return fmt.Errorf("active membership differs from this join")
	}

	previousPath := previousJoinPath(pausePath)
	previous, err := LoadCandidate(previousPath)
	switch {
	case err == nil:
		if previous.Identity() != current.Identity() {
			return fmt.Errorf("saved previous membership differs from this join")
		}
	case errors.Is(err, os.ErrNotExist):
		if active.Identity() == candidate.Identity() {
			return fmt.Errorf("candidate active without previous membership")
		}
		if err := SaveCandidate(previousPath, current); err != nil {
			return fmt.Errorf("save previous membership: %w", err)
		}
	default:
		return fmt.Errorf("load previous membership: %w", err)
	}

	if active.Identity() == candidate.Identity() {
		return nil // Retry after an earlier successful replacement.
	}
	if err := SaveCandidate(activePath, candidate); err != nil {
		return fmt.Errorf("activate committed membership: %w", err)
	}
	return nil
}

// LoadActivatedJoin verifies the saved old membership and the committed,
// paused candidate. Call this only when the active file has advanced.
func LoadActivatedJoin(
	pausePath string,
	active Configuration,
) (Configuration, error) {
	if pausePath == "" {
		return Configuration{}, fmt.Errorf("join pause path is required")
	}

	previous, err := LoadCandidate(previousJoinPath(pausePath))
	if err != nil {
		return Configuration{}, fmt.Errorf(
			"load previous membership: %w", err,
		)
	}
	if _, err := ValidateJoinCandidate(previous, active); err != nil {
		return Configuration{}, fmt.Errorf(
			"active membership is not the saved join: %w", err,
		)
	}

	paused, err := LoadJoinPause(pausePath, previous)
	if err != nil {
		return Configuration{}, fmt.Errorf("load join pause: %w", err)
	}
	committed, err := LoadJoinCommit(pausePath, previous)
	if err != nil {
		return Configuration{}, fmt.Errorf(
			"load join commitment: %w", err,
		)
	}
	if paused.Identity() != active.Identity() ||
		committed.Identity() != active.Identity() {
		return Configuration{}, fmt.Errorf(
			"saved join markers do not match active membership",
		)
	}
	return previous, nil
}

// LoadPreviousJoin returns the old membership saved before activation.
func LoadPreviousJoin(pausePath string) (Configuration, error) {
	if pausePath == "" {
		return Configuration{}, fmt.Errorf("join pause path is required")
	}
	return LoadCandidate(previousJoinPath(pausePath))
}
