package server

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
)

func mustStoreGet(
	t *testing.T,
	m *store.Map,
	key int64,
) (string, bool) {
	t.Helper()

	value, exists, err := m.Get(key)
	if err != nil {
		t.Fatalf("store.Get(%d): %v", key, err)
	}
	return value, exists
}

func mustStoreGetRecord(
	t *testing.T,
	m *store.Map,
	key int64,
) (store.Record, bool) {
	t.Helper()

	record, exists, err := m.GetRecord(key)
	if err != nil {
		t.Fatalf("store.GetRecord(%d): %v", key, err)
	}
	return record, exists
}
