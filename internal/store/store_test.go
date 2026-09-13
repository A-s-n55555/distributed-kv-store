package main

import "testing"
import "sync"

func TestPutAndGet(t *testing.T) {
	m := NewMap()
	m.Put(1, "value1")

	if got,exists := m.Get(1); got != "value1" || !exists {
		t.Fatalf("Get(1) = %q; want %q", got, "value1")
	}
}

func TestPutUpdatesValue(t *testing.T) {
	m := NewMap()
	m.Put(1, "old")
	m.Put(1, "new")

	if got,exists := m.Get(1); got != "new" || !exists{
		t.Fatalf("Get(1) = %q; want %q", got, "new")
	}
}

func TestDelete(t *testing.T) {
	m := NewMap()
	m.Put(1, "value1")
	m.Delete(1)

	if got,exists := m.Get(1); got != "" || exists{
		t.Fatalf("Get(1) after Delete = %q; want empty string", got)
	}
}

func TestConcurrentPut(t *testing.T) {
	m := NewMap()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(key int) {
			defer wg.Done()
			m.Put(key, "value")
		}(i)
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		value, exists := m.Get(i)

		if !exists || value != "value" {
			t.Errorf("key %d was not stored correctly", i)
		}
	}
}
