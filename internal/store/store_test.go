package main

import "testing"

func TestPutAndGet(t *testing.T) {
	m := NewMap()
	m.Put(1, "value1")

	if got := m.Get(1); got != "value1" {
		t.Fatalf("Get(1) = %q; want %q", got, "value1")
	}
}

func TestPutUpdatesValue(t *testing.T) {
	m := NewMap()
	m.Put(1, "old")
	m.Put(1, "new")

	if got := m.Get(1); got != "new" {
		t.Fatalf("Get(1) = %q; want %q", got, "new")
	}
}

func TestDelete(t *testing.T) {
	m := NewMap()
	m.Put(1, "value1")
	m.Delete(1)

	if got := m.Get(1); got != "" {
		t.Fatalf("Get(1) after Delete = %q; want empty string", got)
	}
}
