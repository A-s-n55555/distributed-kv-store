package membership

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func joinResumeDecisionPath(pausePath string) string {
	return filepath.Join(
		filepath.Dir(pausePath),
		"membership-resume-decision.json",
	)
}

// LoadJoinResumeDecision reports whether this exact activated join has
// a saved resume decision. Missing decision means false, nil.
// Invalid or inconsistent files return an error.
func LoadJoinResumeDecision(
	pausePath string,
	active Configuration,
) (bool, error) {
	if pausePath == "" {
		return false, fmt.Errorf("join pause path is required")
	}

	saved, err := LoadCandidate(joinResumeDecisionPath(pausePath))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load resume decision: %w", err)
	}
	if saved.Identity() != active.Identity() {
		return false, fmt.Errorf("resume decision belongs to another membership")
	}

	if _, err := LoadActivatedJoin(pausePath, active); err != nil {
		return false, fmt.Errorf("validate resume activation: %w", err)
	}

	durableActive, err := LoadCandidate(filepath.Join(
		filepath.Dir(pausePath), "membership-active.json",
	))
	if err != nil {
		return false, fmt.Errorf("load active membership: %w", err)
	}
	if durableActive.Identity() != active.Identity() {
		return false, fmt.Errorf("active file differs from resume membership")
	}

	return true, nil
}

// RecordJoinResumeDecision persists the coordinator's authorization to
// begin resume. It does not remove join markers or open any request gates.
//
// Caller must serialize this operation and first verify that every
// candidate member, including the joining node, is activated and paused.
func RecordJoinResumeDecision(
	pausePath, nodeID string,
	active Configuration,
) error {
	if pausePath == "" || nodeID == "" {
		return fmt.Errorf("join pause path and node ID are required")
	}

	previous, err := LoadActivatedJoin(pausePath, active)
	if err != nil {
		return fmt.Errorf("resume requires an activated join: %w", err)
	}
	coordinator, err := JoinCoordinator(previous)
	if err != nil {
		return err
	}
	if coordinator.ID != nodeID {
		return fmt.Errorf(
			"only original coordinator %s can record resume",
			coordinator.ID,
		)
	}

	durableActive, err := LoadCandidate(filepath.Join(
		filepath.Dir(pausePath), "membership-active.json",
	))
	if err != nil {
		return fmt.Errorf("load active membership: %w", err)
	}
	if durableActive.Identity() != active.Identity() {
		return fmt.Errorf("active membership has not advanced to this candidate")
	}

	// Reject a corrupt or conflicting existing decision.
	if _, err := LoadJoinResumeDecision(pausePath, active); err != nil {
		return err
	}

	// Rewriting the same decision is safe and repeats directory sync.
	if err := SaveCandidate(
		joinResumeDecisionPath(pausePath), active,
	); err != nil {
		return fmt.Errorf("persist resume decision: %w", err)
	}
	return nil
}
