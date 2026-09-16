package wal

import (
	"encoding/json"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer log.Close()

	want := []Entry{
		{Operation: "PUT", Key: 1, Value: "value1"},
		{Operation: "PUT", Key: 2, Value: "value2"},
		{Operation: "DELETE", Key: 1},
	}

	for _, entry := range want {
		if err := log.Append(entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	got, err := log.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadAll() = %+v; want %+v", got, want)
	}
}

func TestAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer log.Close()

	want := Entry{
		Operation: "PUT",
		Key:       1,
		Value:     "value1",
	}

	if err := log.Append(want); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("saved entry = %+v; want %+v", got, want)
	}
}

func TestClockRecoveryAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	want := []Entry{
		{
			Operation: "PUT",
			Key:       1,
			Value:     "hello",
			Clock: version.Clock{
				"node-1": 2,
				"node-2": 1,
			},
		},
		{
			Operation: "DELETE",
			Key:       1,
			Clock: version.Clock{
				"node-1": 3,
				"node-2": 1,
			},
		},
	}

	log1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}

	for _, entry := range want {
		if err := log1.Append(entry); err != nil {
			_ = log1.Close()
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := log1.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	// Simulate opening the WAL after a server restart.
	log2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer log2.Close()

	got, err := log2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered entries = %+v; want %+v", got, want)
	}
}

func TestReadLegacyEntryWithoutClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// This matches the previous WAL format.
	legacyData := []byte(
		"{\"operation\":\"PUT\",\"key\":1,\"value\":\"old-value\"}\n",
	)

	if err := os.WriteFile(path, legacyData, 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	log, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer log.Close()

	entries, err := log.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("entry count = %d; want 1", len(entries))
	}

	entry := entries[0]

	if entry.Operation != "PUT" ||
		entry.Key != 1 ||
		entry.Value != "old-value" {
		t.Fatalf("legacy entry was recovered incorrectly: %+v", entry)
	}

	if entry.Clock != nil {
		t.Fatalf("legacy clock = %v; want nil", entry.Clock)
	}
}
