package handoff

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
)

type queueEvent struct {
	Operation string `json:"operation"`
	Hint      *Hint  `json:"hint,omitempty"`
	ID        string `json:"id,omitempty"`
}

type Queue struct {
	mu      sync.Mutex
	file    *os.File
	pending map[string]Hint
	closed  bool
	failure error
}

// OpenQueue replays the journal and restores pending deliveries.
// Only one Queue/process should own a journal path at a time.
func OpenQueue(path string) (*Queue, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		file:    file,
		pending: make(map[string]Hint),
	}

	decoder := json.NewDecoder(file)

	for {
		var event queueEvent

		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}

		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read hint journal: %w", err)
		}

		if err := q.replay(event); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return q, nil
}

func (q *Queue) replay(event queueEvent) error {
	switch event.Operation {
	case "ENQUEUE":
		if event.Hint == nil {
			return fmt.Errorf("ENQUEUE event has no hint")
		}

		if err := event.Hint.Validate(); err != nil {
			return fmt.Errorf("invalid journal hint: %w", err)
		}

		hint := event.Hint.Clone()

		if existing, exists := q.pending[hint.ID]; exists &&
			!reflect.DeepEqual(existing, hint) {
			return fmt.Errorf("conflicting hint ID: %s", hint.ID)
		}

		q.pending[hint.ID] = hint

	case "ACK":
		if event.ID == "" {
			return fmt.Errorf("ACK event has no hint ID")
		}

		delete(q.pending, event.ID)

	default:
		return fmt.Errorf(
			"unknown hint journal operation: %q",
			event.Operation,
		)
	}

	return nil
}

func (q *Queue) writableLocked() error {
	if q.closed {
		return fmt.Errorf("hint queue is closed")
	}

	if q.failure != nil {
		return fmt.Errorf(
			"hint queue stopped after a persistence failure: %w",
			q.failure,
		)
	}

	return nil
}

func (q *Queue) appendLocked(event queueEvent) error {
	if err := json.NewEncoder(q.file).Encode(event); err != nil {
		q.failure = err
		return err
	}

	if err := q.file.Sync(); err != nil {
		q.failure = err
		return err
	}

	return nil
}

// Enqueue persists the hint before making it available for delivery.
func (q *Queue) Enqueue(hint Hint) error {
	if err := hint.Validate(); err != nil {
		return err
	}

	hint = hint.Clone()

	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.writableLocked(); err != nil {
		return err
	}

	if existing, exists := q.pending[hint.ID]; exists {
		if reflect.DeepEqual(existing, hint) {
			return nil
		}

		return fmt.Errorf("conflicting hint ID: %s", hint.ID)
	}

	if err := q.appendLocked(queueEvent{
		Operation: "ENQUEUE",
		Hint:      &hint,
	}); err != nil {
		return err
	}

	q.pending[hint.ID] = hint
	return nil
}

// Ack records completion before removing the pending hint.
func (q *Queue) Ack(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.writableLocked(); err != nil {
		return err
	}

	if _, exists := q.pending[id]; !exists {
		return nil
	}

	if err := q.appendLocked(queueEvent{
		Operation: "ACK",
		ID:        id,
	}); err != nil {
		return err
	}

	delete(q.pending, id)
	return nil
}

// Pending returns independent snapshots in deterministic ID order.
func (q *Queue) Pending() []Hint {
	q.mu.Lock()
	defer q.mu.Unlock()

	hints := make([]Hint, 0, len(q.pending))

	for _, hint := range q.pending {
		hints = append(hints, hint.Clone())
	}

	sort.Slice(hints, func(i, j int) bool {
		return hints[i].ID < hints[j].ID
	})

	return hints
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	q.closed = true
	return q.file.Close()
}
