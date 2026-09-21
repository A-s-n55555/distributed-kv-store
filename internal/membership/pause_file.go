package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SaveJoinPause durably records one validated join candidate.
// Repeating the same request is safe; a different existing marker is rejected.
func SaveJoinPause(path string, current, candidate Configuration) error {
	if _, err := ValidateJoinCandidate(current, candidate); err != nil {
		return fmt.Errorf("invalid join pause candidate: %w", err)
	}

	existing, err := LoadJoinPause(path, current)
	if err == nil {
		if existing.Identity() != candidate.Identity() {
			return fmt.Errorf("join pause already belongs to another candidate")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect existing join pause: %w", err)
	}

	return SaveCandidate(path, candidate)
}

// LoadJoinPause reads the marker and checks it against active membership.
// A missing marker returns an error wrapping os.ErrNotExist.
func LoadJoinPause(path string, current Configuration) (Configuration, error) {
	candidate, _, err := LoadJoinCandidate(path, current)
	if err != nil {
		return Configuration{}, fmt.Errorf("load join pause: %w", err)
	}
	return candidate, nil
}

// ClearJoinPause removes only the marker for the expected candidate.
func ClearJoinPause(
	path string,
	current Configuration,
	expected Identity,
) error {
	candidate, err := LoadJoinPause(path, current)
	if err != nil {
		return err
	}
	if candidate.Identity() != expected {
		return fmt.Errorf("join pause identity does not match")
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove join pause: %w", err)
	}

	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open join pause directory: %w", err)
	}
	defer directory.Close()

	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync join pause directory: %w", err)
	}
	return nil
}
