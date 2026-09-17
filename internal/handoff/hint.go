package handoff

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

// Hint describes a versioned record awaiting delivery to a replica.
type Hint struct {
	ID     string       `json:"id"`
	Target ring.Node    `json:"target"`
	Key    int64        `json:"key"`
	Record store.Record `json:"record"`
}

// NewHint validates the operation and creates an independent snapshot.
func NewHint(
	target ring.Node,
	key int64,
	record store.Record,
) (Hint, error) {
	hint := Hint{
		Target: target,
		Key:    key,
		Record: record,
	}

	if err := validatePayload(hint); err != nil {
		return Hint{}, err
	}

	var randomID [16]byte

	if _, err := rand.Read(randomID[:]); err != nil {
		return Hint{}, fmt.Errorf("generate hint ID: %w", err)
	}

	hint.ID = hex.EncodeToString(randomID[:])
	hint.Record.Clock = version.Clone(record.Clock)

	return hint, nil
}

// Clone prevents sharing the mutable clock map.
func (h Hint) Clone() Hint {
	h.Record.Clock = version.Clone(h.Record.Clock)
	return h
}

// Validate checks a complete hint, including one recovered from disk.
func (h Hint) Validate() error {
	if h.ID == "" {
		return fmt.Errorf("hint ID is required")
	}

	return validatePayload(h)
}

func validatePayload(h Hint) error {
	if h.Target.ID == "" || h.Target.Address == "" {
		return fmt.Errorf("target node ID and address are required")
	}

	if len(h.Record.Clock) == 0 {
		return fmt.Errorf("hint requires a versioned record")
	}

	for nodeID, counter := range h.Record.Clock {
		if nodeID == "" || counter == 0 {
			return fmt.Errorf("hint contains an invalid vector clock")
		}
	}

	if h.Record.Deleted && h.Record.Value != "" {
		return fmt.Errorf("hint tombstone must have an empty value")
	}

	return nil
}
