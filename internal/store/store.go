package store

import (
	"errors"
	"fmt"
	"sync"
	"reflect"

	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
)

var ErrRecordConflict = errors.New("record version conflict")

type Record struct {
	Value   string
	Clock   version.Clock
	Deleted bool
}

type Map struct {
	mu       sync.RWMutex
	data map[int64][]Record
	counters map[string]uint64
	wal      *wal.Log
}

func NewMap(log *wal.Log) (*Map, error) {
	entries, err := log.ReadAll()
	if err != nil {
		return nil, err
	}

	m := &Map{
		data:     make(map[int64][]Record),
		counters: make(map[string]uint64),
		wal:      log,
	}

	for index, entry := range entries {
		m.observeClockLocked(entry.Clock)

		record := Record{
			Clock: version.Clone(entry.Clock),
		}

		switch entry.Operation {
		case "PUT":
			record.Value = entry.Value

		case "DELETE":
			record.Deleted = true

		case "CLOCK":
			// A counter reservation does not create a key.
			continue

		default:
			return nil, fmt.Errorf(
				"unknown WAL operation: %q",
				entry.Operation,
			)
		}

		if len(record.Clock) == 0 {
			// Legacy, unversioned writes use chronological replay.
			// Never let one erase an already-versioned key.
			if hasVersionedRecords(m.data[entry.Key]) {
				return nil, fmt.Errorf(
					"WAL entry %d: unversioned write after versioned key %d",
					index+1,
					entry.Key,
				)
			}

			m.data[entry.Key] = []Record{record}
			continue
		}

		if err := validateVersionedRecord(record); err != nil {
			return nil, fmt.Errorf(
				"WAL entry %d: %w",
				index+1,
				err,
			)
		}

		merged, err := mergeRecordVersions(
			m.data[entry.Key],
			record,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"WAL entry %d, key %d: %w",
				index+1,
				entry.Key,
				err,
			)
		}

		m.data[entry.Key] = merged
	}

	return m, nil
}

func hasVersionedRecords(records []Record) bool {
	for _, record := range records {
		if len(record.Clock) > 0 {
			return true
		}
	}
	return false
}

func validateVersionedRecord(record Record) error {
	if len(record.Clock) == 0 {
		return fmt.Errorf("incoming record must have a vector clock")
	}

	for nodeID, counter := range record.Clock {
		if nodeID == "" || counter == 0 {
			return fmt.Errorf(
				"invalid clock entry: node=%q counter=%d",
				nodeID,
				counter,
			)
		}
	}

	if record.Deleted && record.Value != "" {
		return fmt.Errorf("a tombstone must have an empty value")
	}

	return nil
}

func (m *Map) Put(key int64, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if hasVersionedRecords(m.data[key]) {
		return fmt.Errorf(
			"key %d is versioned; use ApplyRecord",
			key,
		)
	}

	if err := m.wal.Append(wal.Entry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
	}); err != nil {
		return err
	}

	m.data[key] = []Record{{Value: value}}
	return nil
}

func (m *Map) Delete(key int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if hasVersionedRecords(m.data[key]) {
		return fmt.Errorf(
			"key %d is versioned; use ApplyRecord",
			key,
		)
	}

	if err := m.wal.Append(wal.Entry{
		Operation: "DELETE",
		Key:       key,
	}); err != nil {
		return err
	}

	m.data[key] = []Record{{Deleted: true}}
	return nil
}

// GetRecords returns all retained siblings with independent clocks.
func (m *Map) GetRecords(key int64) []Record {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stored := m.data[key]
	if len(stored) == 0 {
		return nil
	}

	result := make([]Record, len(stored))
	for i, record := range stored {
		result[i] = record
		if record.Clock != nil {
			result[i].Clock = version.Clone(record.Clock)
		}
	}
	return result
}

// GetRecord returns a single version, including a tombstone.
// Multiple siblings produce an explicit conflict, not an arbitrary winner.
func (m *Map) GetRecord(key int64) (Record, bool, error) {
	records := m.GetRecords(key)

	switch len(records) {
	case 0:
		return Record{}, false, nil

	case 1:
		return records[0], true, nil

	default:
		return Record{}, true, fmt.Errorf(
			"%w: key %d has %d siblings",
			ErrRecordConflict,
			key,
			len(records),
		)
	}
}

func (m *Map) Get(key int64) (string, bool, error) {
	record, exists, err := m.GetRecord(key)
	if err != nil {
		return "", exists, err
	}

	if !exists || record.Deleted {
		return "", false, nil
	}

	return record.Value, true, nil
}

func (m *Map) ApplyRecord(key int64, incoming Record) error {
	if err := validateVersionedRecord(incoming); err != nil {
		return err
	}

	incoming.Clock = version.Clone(incoming.Clock)

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.data[key]

	merged, err := mergeRecordVersions(current, incoming)
	if err != nil {
		return fmt.Errorf("key %d: %w", key, err)
	}

	// Duplicate or dominated input: no extra WAL entry.
	if reflect.DeepEqual(current, merged) {
		return nil
	}

	operation := "PUT"
	if incoming.Deleted {
		operation = "DELETE"
	}

	// Persist before publishing the new sibling set.
	if err := m.wal.Append(wal.Entry{
		Operation: operation,
		Key:       key,
		Value:     incoming.Value,
		Clock:     incoming.Clock,
	}); err != nil {
		return err
	}

	m.data[key] = merged
	m.observeClockLocked(incoming.Clock)
	return nil
}
// observeClockLocked tracks the highest counters seen.
// The caller must hold m.mu or be initializing an unpublished store.
func (m *Map) observeClockLocked(clock version.Clock) {
	for nodeID, counter := range clock {
		if counter > m.counters[nodeID] {
			m.counters[nodeID] = counter
		}
	}
}

// NextClock durably reserves a fresh coordinator counter.
func (m *Map) NextClock(
	nodeID string,
	observed version.Clock,
) (version.Clock, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("coordinator node ID is required")
	}

	for observedNodeID, counter := range observed {
		if observedNodeID == "" || counter == 0 {
			return nil, fmt.Errorf("invalid observed vector clock")
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	counter := m.counters[nodeID]

	if observed[nodeID] > counter {
		counter = observed[nodeID]
	}

	if counter == ^uint64(0) {
		return nil, fmt.Errorf("vector clock counter overflow")
	}

	next := version.Clone(observed)
	next[nodeID] = counter + 1

	// Persist the reservation before it can be used remotely.
	if err := m.wal.Append(wal.Entry{
		Operation: "CLOCK",
		Clock:     next,
	}); err != nil {
		return nil, err
	}

	m.observeClockLocked(next)
	return next, nil
}
