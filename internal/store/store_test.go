package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
)

func newTestMap(t *testing.T) *Map {
	t.Helper()

	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := wal.Open(path)
	if err != nil {
		t.Fatalf("wal.Open() error = %v", err)
	}

	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Errorf("wal.Close() error = %v", err)
		}
	})

	m, err := NewMap(log)
	if err != nil {
		t.Fatalf("NewMap() error = %v", err)
	}

	return m
}

func TestPutAndGet(t *testing.T) {
	m := newTestMap(t)

	if err := m.Put(1, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if got, exists := m.Get(1); got != "value1" || !exists {
		t.Fatalf("Get(1) = %q, exists = %v; want %q, true",
			got, exists, "value1")
	}
}

func TestPutUpdatesValue(t *testing.T) {
	m := newTestMap(t)

	if err := m.Put(1, "old"); err != nil {
		t.Fatalf("first Put() error = %v", err)
	}

	if err := m.Put(1, "new"); err != nil {
		t.Fatalf("second Put() error = %v", err)
	}

	if got, exists := m.Get(1); got != "new" || !exists {
		t.Fatalf("Get(1) = %q, exists = %v; want %q, true",
			got, exists, "new")
	}
}

func TestDelete(t *testing.T) {
	m := newTestMap(t)

	if err := m.Put(1, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := m.Delete(1); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if got, exists := m.Get(1); got != "" || exists {
		t.Fatalf("Get(1) after Delete = %q, exists = %v; want empty string, false",
			got, exists)
	}
}

func TestConcurrentPut(t *testing.T) {
	m := newTestMap(t)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(key int) {
			defer wg.Done()

			if err := m.Put(int64(key), "value"); err != nil {
				t.Errorf("Put(%d) error = %v", key, err)
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		if value, exists := m.Get(int64(i)); value != "value" || !exists {
			t.Errorf("key %d was not stored correctly", i)
		}
	}
}

func TestRecoveryFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// First application run.
	log1, err := wal.Open(path)
	if err != nil {
		t.Fatalf("first wal.Open() error = %v", err)
	}

	store1, err := NewMap(log1)
	if err != nil {
		t.Fatalf("first NewMap() error = %v", err)
	}

	if err := store1.Put(1, "value1"); err != nil {
		t.Fatalf("Put(1) error = %v", err)
	}

	if err := store1.Put(2, "value2"); err != nil {
		t.Fatalf("Put(2) error = %v", err)
	}

	if err := store1.Delete(1); err != nil {
		t.Fatalf("Delete(1) error = %v", err)
	}

	if err := log1.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	// Simulated application restart.
	log2, err := wal.Open(path)
	if err != nil {
		t.Fatalf("second wal.Open() error = %v", err)
	}
	defer log2.Close()

	store2, err := NewMap(log2)
	if err != nil {
		t.Fatalf("second NewMap() error = %v", err)
	}

	if value, exists := store2.Get(1); exists || value != "" {
		t.Errorf("deleted key 1 was restored")
	}

	if value, exists := store2.Get(2); !exists || value != "value2" {
		t.Errorf("Get(2) = %q, exists = %v; want value2, true",
			value, exists)
	}
	record, exists := store2.GetRecord(1)
	if !exists || !record.Deleted {
		t.Fatal("deleted key's tombstone was not recovered")
	}
}
func TestDeleteRetainsTombstone(t *testing.T) {
	m := newTestMap(t)

	if err := m.Put(1, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := m.Delete(1); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	record, exists := m.GetRecord(1)

	if !exists || !record.Deleted {
		t.Fatalf(
			"GetRecord() = %+v, exists=%v; want tombstone",
			record,
			exists,
		)
	}

	if value, found := m.Get(1); found || value != "" {
		t.Fatal("Get() exposed a deleted record")
	}
}

func TestRecoveryPreservesRecordMetadata(t *testing.T) {
	m := newTestMap(t)

	entries := []wal.Entry{
		{
			Operation: "PUT",
			Key:       1,
			Value:     "hello",
			Clock:     version.Clock{"node-1": 2},
		},
		{
			Operation: "DELETE",
			Key:       2,
			Clock:     version.Clock{"node-1": 3},
		},
	}

	for _, entry := range entries {
		if err := m.wal.Append(entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Rebuild the store from its persisted WAL.
	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatalf("NewMap() error = %v", err)
	}

	valueRecord, exists := recovered.GetRecord(1)
	if !exists ||
		valueRecord.Deleted ||
		valueRecord.Value != "hello" ||
		valueRecord.Clock["node-1"] != 2 {
		t.Fatalf("incorrect recovered value: %+v", valueRecord)
	}

	tombstone, exists := recovered.GetRecord(2)
	if !exists ||
		!tombstone.Deleted ||
		tombstone.Clock["node-1"] != 3 {
		t.Fatalf("incorrect recovered tombstone: %+v", tombstone)
	}

	// Modifying the returned clock must not change stored metadata.
	valueRecord.Clock["node-1"] = 999

	unchanged, _ := recovered.GetRecord(1)
	if unchanged.Clock["node-1"] != 2 {
		t.Fatal("GetRecord() exposed the internal clock map")
	}
}

func TestApplyRecordPersistsClockAndCopiesInput(t *testing.T) {
	m := newTestMap(t)

	incoming := Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}

	if err := m.ApplyRecord(1, incoming); err != nil {
		t.Fatalf("ApplyRecord() error = %v", err)
	}

	incoming.Clock["node-1"] = 999

	record, _ := m.GetRecord(1)
	if record.Clock["node-1"] != 1 {
		t.Fatal("ApplyRecord() retained the caller's clock map")
	}

	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatalf("NewMap() error = %v", err)
	}

	record, exists := recovered.GetRecord(1)
	if !exists ||
		record.Value != "hello" ||
		record.Clock["node-1"] != 1 {
		t.Fatalf("incorrect recovered record: %+v", record)
	}
}

func TestApplyRecordIgnoresStaleAndDuplicateRecords(t *testing.T) {
	m := newTestMap(t)

	newer := Record{
		Value: "new",
		Clock: version.Clock{"node-1": 2},
	}

	if err := m.ApplyRecord(1, newer); err != nil {
		t.Fatal(err)
	}

	if err := m.ApplyRecord(1, newer); err != nil {
		t.Fatalf("duplicate record error = %v", err)
	}

	older := Record{
		Value: "old",
		Clock: version.Clock{"node-1": 1},
	}

	if err := m.ApplyRecord(1, older); err != nil {
		t.Fatalf("stale record error = %v", err)
	}

	value, found := m.Get(1)
	if !found || value != "new" {
		t.Fatalf("Get(1) = %q, %v; want new, true", value, found)
	}

	entries, err := m.wal.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 1 {
		t.Fatalf("WAL entries = %d; want 1", len(entries))
	}
}

func TestApplyRecordRejectsConflicts(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	conflicts := []Record{
		{
			Value: "concurrent",
			Clock: version.Clock{"node-2": 1},
		},
		{
			Value: "different",
			Clock: version.Clock{"node-1": 1},
		},
	}

	for _, incoming := range conflicts {
		err := m.ApplyRecord(1, incoming)

		if !errors.Is(err, ErrRecordConflict) {
			t.Fatalf("error = %v; want ErrRecordConflict", err)
		}
	}

	value, _ := m.Get(1)
	if value != "first" {
		t.Fatal("conflicting record replaced the existing value")
	}
}

func TestVersionedTombstonePreventsStaleResurrection(t *testing.T) {
	m := newTestMap(t)

	oldValue := Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}

	if err := m.ApplyRecord(1, oldValue); err != nil {
		t.Fatal(err)
	}

	if err := m.ApplyRecord(1, Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := m.ApplyRecord(1, oldValue); err != nil {
		t.Fatal(err)
	}

	if _, found := m.Get(1); found {
		t.Fatal("stale value resurrected a deleted key")
	}

	if err := m.Put(1, "unversioned"); err == nil {
		t.Fatal("Put() bypassed the versioned tombstone")
	}

	if err := m.Delete(1); err == nil {
		t.Fatal("Delete() bypassed the versioned record guard")
	}

	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatal(err)
	}

	record, exists := recovered.GetRecord(1)
	if !exists ||
		!record.Deleted ||
		record.Clock["node-1"] != 2 {
		t.Fatalf("incorrect recovered tombstone: %+v", record)
	}
}
func TestNextClockPersistsReservations(t *testing.T) {
	m := newTestMap(t)

	first, err := m.NextClock("node-1", nil)
	if err != nil {
		t.Fatal(err)
	}

	if first["node-1"] != 1 {
		t.Fatalf("first counter = %d; want 1", first["node-1"])
	}

	second, err := m.NextClock("node-1", nil)
	if err != nil {
		t.Fatal(err)
	}

	if second["node-1"] != 2 {
		t.Fatalf("second counter = %d; want 2", second["node-1"])
	}

	observed := version.Clock{
		"node-1": 5,
		"node-2": 3,
	}

	next, err := m.NextClock("node-1", observed)
	if err != nil {
		t.Fatal(err)
	}

	if next["node-1"] != 6 || next["node-2"] != 3 {
		t.Fatalf("incorrect next clock: %v", next)
	}

	if observed["node-1"] != 5 {
		t.Fatal("NextClock() modified its input")
	}

	// Replay reservations even though no value was written.
	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatal(err)
	}

	afterRecovery, err := recovered.NextClock("node-1", nil)
	if err != nil {
		t.Fatal(err)
	}

	if afterRecovery["node-1"] != 7 {
		t.Fatalf(
			"recovered counter = %d; want 7",
			afterRecovery["node-1"],
		)
	}

	if _, exists := recovered.GetRecord(0); exists {
		t.Fatal("CLOCK entry incorrectly created a key record")
	}
}

func TestNextClockObservesAppliedRecords(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 10},
	}); err != nil {
		t.Fatal(err)
	}

	next, err := m.NextClock("node-1", nil)
	if err != nil {
		t.Fatal(err)
	}

	if next["node-1"] != 11 {
		t.Fatalf("counter = %d; want 11", next["node-1"])
	}
}

func TestNextClockRejectsOverflow(t *testing.T) {
	m := newTestMap(t)

	_, err := m.NextClock(
		"node-1",
		version.Clock{"node-1": ^uint64(0)},
	)

	if err == nil {
		t.Fatal("NextClock() accepted counter overflow")
	}
}
