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

	if got, exists := mustGet(t, m, 1); got != "value1" || !exists {
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

	if got, exists := mustGet(t, m, 1); got != "new" || !exists {
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

	if got, exists := mustGet(t, m, 1); got != "" || exists {
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
		if value, exists := mustGet(t, m, int64(i)); value != "value" || !exists {
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

	if value, exists := mustGet(t, store2, 1); exists || value != "" {
		t.Errorf("deleted key 1 was restored")
	}

	if value, exists := mustGet(t, store2, 2); !exists || value != "value2" {
		t.Errorf("Get(2) = %q, exists = %v; want value2, true",
			value, exists)
	}
	record, exists := mustGetRecord(t, store2, 1)
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

	record, exists := mustGetRecord(t, m, 1)

	if !exists || !record.Deleted {
		t.Fatalf(
			"GetRecord() = %+v, exists=%v; want tombstone",
			record,
			exists,
		)
	}

	if value, found := mustGet(t, m, 1); found || value != "" {
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

	valueRecord, exists := mustGetRecord(t, recovered, 1)
	if !exists ||
		valueRecord.Deleted ||
		valueRecord.Value != "hello" ||
		valueRecord.Clock["node-1"] != 2 {
		t.Fatalf("incorrect recovered value: %+v", valueRecord)
	}

	tombstone, exists := mustGetRecord(t, recovered, 2)
	if !exists ||
		!tombstone.Deleted ||
		tombstone.Clock["node-1"] != 3 {
		t.Fatalf("incorrect recovered tombstone: %+v", tombstone)
	}

	// Modifying the returned clock must not change stored metadata.
	valueRecord.Clock["node-1"] = 999

	unchanged, _ := mustGetRecord(t, recovered, 1)
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

	record, _ := mustGetRecord(t, m, 1)
	if record.Clock["node-1"] != 1 {
		t.Fatal("ApplyRecord() retained the caller's clock map")
	}

	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatalf("NewMap() error = %v", err)
	}

	record, exists := mustGetRecord(t, recovered, 1)
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

	value, found := mustGet(t, m, 1)
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

func TestApplyRecordRejectsEqualClockDifferentContents(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	err := m.ApplyRecord(1, Record{
		Value: "different",
		Clock: version.Clock{"node-1": 1},
	})
	if !errors.Is(err, ErrRecordConflict) {
		t.Fatalf("error = %v; want ErrRecordConflict", err)
	}

	value, exists := mustGet(t, m, 1)
	if !exists || value != "first" {
		t.Fatal("invalid version changed the stored value")
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

	if _, found := mustGet(t, m, 1); found {
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

	record, exists := mustGetRecord(t, recovered, 1)
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

	if _, exists := mustGetRecord(t, recovered, 0); exists {
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
func TestGetRecordsCopiesClock(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	records := m.GetRecords(1)

	if len(records) != 1 || records[0].Value != "hello" {
		t.Fatalf("incorrect records: %+v", records)
	}

	records[0].Clock["node-1"] = 999

	unchanged := m.GetRecords(1)
	if unchanged[0].Clock["node-1"] != 1 {
		t.Fatal("GetRecords() exposed the internal clock")
	}

	if len(m.GetRecords(99)) != 0 {
		t.Fatal("missing key returned records")
	}
}

func mustGet(
	t *testing.T,
	m *Map,
	key int64,
) (string, bool) {
	t.Helper()

	value, exists, err := m.Get(key)
	if err != nil {
		t.Fatalf("Get(%d): %v", key, err)
	}
	return value, exists
}

func mustGetRecord(
	t *testing.T,
	m *Map,
	key int64,
) (Record, bool) {
	t.Helper()

	record, exists, err := m.GetRecord(key)
	if err != nil {
		t.Fatalf("GetRecord(%d): %v", key, err)
	}
	return record, exists
}

func TestStorePreservesSiblingsAcrossRecovery(t *testing.T) {
	m := newTestMap(t)

	first := Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}
	second := Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}

	for _, record := range []Record{first, second} {
		if err := m.ApplyRecord(1, record); err != nil {
			t.Fatal(err)
		}
	}

	records := m.GetRecords(1)
	if len(records) != 2 {
		t.Fatalf("siblings = %d; want 2", len(records))
	}

	if _, _, err := m.Get(1); !errors.Is(err, ErrRecordConflict) {
		t.Fatalf("Get error = %v; want conflict", err)
	}

	if _, _, err := m.GetRecord(1); !errors.Is(err, ErrRecordConflict) {
		t.Fatalf("GetRecord error = %v; want conflict", err)
	}

	// Duplicate delivery must not append another record.
	if err := m.ApplyRecord(1, second); err != nil {
		t.Fatal(err)
	}

	entries, err := m.wal.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("WAL entries = %d; want 2", len(entries))
	}

	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatal(err)
	}

	records = recovered.GetRecords(1)
	if len(records) != 2 ||
		records[0].Value != "first" ||
		records[1].Value != "second" {
		t.Fatalf("incorrect recovered siblings: %+v", records)
	}

	records[0].Clock["node-1"] = 999
	if recovered.GetRecords(1)[0].Clock["node-1"] != 1 {
		t.Fatal("GetRecords exposed an internal clock")
	}
}

func TestDominatingRecordReplacesSiblings(t *testing.T) {
	m := newTestMap(t)

	for _, record := range []Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "second",
			Clock: version.Clock{"node-2": 1},
		},
		{
			Value: "resolved",
			Clock: version.Clock{
				"node-1": 2,
				"node-2": 1,
			},
		},
	} {
		if err := m.ApplyRecord(1, record); err != nil {
			t.Fatal(err)
		}
	}

	value, exists := mustGet(t, m, 1)
	if !exists || value != "resolved" {
		t.Fatal("dominating version did not replace siblings")
	}

	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatal(err)
	}

	records := recovered.GetRecords(1)
	if len(records) != 1 || records[0].Value != "resolved" {
		t.Fatalf("incorrect recovered resolution: %+v", records)
	}
}

func TestConcurrentTombstoneRemainsSibling(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "live",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := m.ApplyRecord(1, Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewMap(m.wal)
	if err != nil {
		t.Fatal(err)
	}

	records := recovered.GetRecords(1)
	if len(records) != 2 ||
		records[0].Deleted ||
		!records[1].Deleted {
		t.Fatalf("incorrect live/delete siblings: %+v", records)
	}

	if _, _, err := recovered.Get(1); !errors.Is(err, ErrRecordConflict) {
		t.Fatalf("Get error = %v; want conflict", err)
	}
}
