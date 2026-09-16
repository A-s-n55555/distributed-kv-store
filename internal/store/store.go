package store

import (
	"errors"
	"fmt"
	"sync"

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
	data     map[int64]Record
	counters map[string]uint64
	wal      *wal.Log
}

func NewMap(log *wal.Log) (*Map, error) {
	entries, err := log.ReadAll()
	if err != nil {
		return nil, err
	}

	m := &Map{
		data:     make(map[int64]Record),
		counters: make(map[string]uint64),
		wal:      log,
	}

	for _, entry := range entries {
		m.observeClockLocked(entry.Clock)
		record := Record{}

		if entry.Clock != nil {
			record.Clock = version.Clone(entry.Clock)
		}

		switch entry.Operation {
		case "PUT":
			record.Value = entry.Value

		case "DELETE":
			record.Deleted = true

		case "CLOCK":
			// Counter allocation only; do not create a key record.
			continue

		default:
			return nil, fmt.Errorf(
				"unknown WAL operation: %q",
				entry.Operation,
			)
		}

		m.data[entry.Key] = record
	}

	return m, nil
}

func (m *Map) Put(key int64, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if current, exists := m.data[key]; exists &&
		len(current.Clock) > 0 {
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

	// Existing API writes remain unversioned until clock integration.
	m.data[key] = Record{
		Value: value,
	}

	return nil
}

func (m *Map) Get(key int64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, exists := m.data[key]

	if !exists || record.Deleted {
		return "", false
	}

	return record.Value, true
}

// GetRecord returns metadata, including deletion tombstones.
// The returned clock is copied to protect the internal map.
func (m *Map) GetRecord(key int64) (Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, exists := m.data[key]
	if !exists {
		return Record{}, false
	}

	if record.Clock != nil {
		record.Clock = version.Clone(record.Clock)
	}

	return record, true
}

func (m *Map) Delete(key int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if current, exists := m.data[key]; exists &&
		len(current.Clock) > 0 {
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

	// Retain the deletion rather than removing the record.
	m.data[key] = Record{
		Deleted: true,
	}

	return nil
}

func (m *Map) ApplyRecord(key int64, incoming Record) error {
	if len(incoming.Clock) == 0 {
		return fmt.Errorf("incoming record must have a vector clock")
	}

	for nodeID, counter := range incoming.Clock {
		if nodeID == "" || counter == 0 {
			return fmt.Errorf(
				"invalid clock entry: node=%q counter=%d",
				nodeID,
				counter,
			)
		}
	}

	if incoming.Deleted && incoming.Value != "" {
		return fmt.Errorf("a tombstone must have an empty value")
	}

	// Do not retain a caller-owned map.
	incoming.Clock = version.Clone(incoming.Clock)

	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.data[key]

	if exists {
		switch version.Compare(incoming.Clock, current.Clock) {
		case version.Before:
			// A stale record must not replace newer data.
			return nil

		case version.Equal:
			if incoming.Value != current.Value ||
				incoming.Deleted != current.Deleted {
				return fmt.Errorf(
					"%w: equal clocks have different contents for key %d",
					ErrRecordConflict,
					key,
				)
			}

			// Identical version: no additional WAL entry needed.
			return nil

		case version.Concurrent:
			return fmt.Errorf(
				"%w: concurrent versions for key %d",
				ErrRecordConflict,
				key,
			)

		case version.After:
			// Continue and persist the newer record.
		}
	}

	operation := "PUT"
	if incoming.Deleted {
		operation = "DELETE"
	}

	if err := m.wal.Append(wal.Entry{
		Operation: operation,
		Key:       key,
		Value:     incoming.Value,
		Clock:     incoming.Clock,
	}); err != nil {
		return err
	}

	m.data[key] = incoming
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
