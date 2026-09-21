package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func joinCommitPath(pausePath string) (string, error) {
	if pausePath == "" {
		return "", fmt.Errorf("join pause path is required")
	}
	return filepath.Join(
		filepath.Dir(pausePath), "membership-join-commit.json",
	), nil
}

// LoadJoinCommit returns os.ErrNotExist when no commit decision exists.
func LoadJoinCommit(
	pausePath string,
	current Configuration,
) (Configuration, error) {
	path, err := joinCommitPath(pausePath)
	if err != nil {
		return Configuration{}, err
	}
	candidate, _, err := LoadJoinCandidate(path, current)
	if err != nil {
		return Configuration{}, fmt.Errorf("load join commitment: %w", err)
	}
	return candidate, nil
}

// RecordJoinCommit makes one paused candidate the durable join decision.
func RecordJoinCommit(
	pausePath string,
	current, candidate Configuration,
) error {
	if _, err := ValidateJoinCandidate(current, candidate); err != nil {
		return err
	}

	paused, err := LoadJoinPause(pausePath, current)
	if err != nil {
		return fmt.Errorf("join must be durably paused: %w", err)
	}
	if paused.Identity() != candidate.Identity() {
		return fmt.Errorf("join pause belongs to another candidate")
	}

	aborted, err := JoinWasAborted(pausePath, current, candidate)
	if err != nil {
		return err
	}
	if aborted {
		return fmt.Errorf("aborted join cannot be committed")
	}

	existing, err := LoadJoinCommit(pausePath, current)
	if err == nil {
		if existing.Identity() != candidate.Identity() {
			return fmt.Errorf("another join is already committed")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	path, err := joinCommitPath(pausePath)
	if err != nil {
		return err
	}
	return SaveCandidate(path, candidate)
}
