package store

import (
	"path/filepath"
	"sync"
	"testing"

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
}
