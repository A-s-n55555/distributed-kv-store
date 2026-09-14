package wal

import (
	"encoding/json"
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

	if got != want {
		t.Errorf("saved entry = %+v; want %+v", got, want)
	}
}
