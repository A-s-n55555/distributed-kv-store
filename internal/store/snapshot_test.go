package store

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestSnapshotIncludesSiblingsAndTombstones(t *testing.T) {
	m := newTestMap(t)

	first := Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}
	second := Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}
	tombstone := Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 2},
	}

	for _, record := range []Record{first, second} {
		if err := m.ApplyRecord(1, record); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.ApplyRecord(2, tombstone); err != nil {
		t.Fatal(err)
	}

	if err := m.Put(3, "legacy"); err != nil {
		t.Fatal(err)
	}

	snapshot := m.Snapshot()

	if len(snapshot) != 3 {
		t.Fatalf("snapshot keys = %d; want 3", len(snapshot))
	}

	if len(snapshot[1]) != 2 {
		t.Fatalf("key 1 siblings = %d; want 2", len(snapshot[1]))
	}

	for _, expected := range []Record{first, second} {
		found := false

		for _, actual := range snapshot[1] {
			if actual.Value == expected.Value &&
				actual.Deleted == expected.Deleted &&
				version.Compare(
					actual.Clock,
					expected.Clock,
				) == version.Equal {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("snapshot missing sibling %+v", expected)
		}
	}

	deleted := snapshot[2]
	if len(deleted) != 1 ||
		!deleted[0].Deleted ||
		deleted[0].Clock["node-1"] != 2 {
		t.Fatalf("incorrect tombstone: %+v", deleted)
	}

	legacy := snapshot[3]
	if len(legacy) != 1 ||
		legacy[0].Value != "legacy" ||
		len(legacy[0].Clock) != 0 {
		t.Fatalf("incorrect legacy record: %+v", legacy)
	}
}

func TestSnapshotDoesNotExposeStoreState(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "original",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	snapshot := m.Snapshot()

	snapshot[1][0].Value = "changed"
	snapshot[1][0].Clock["node-1"] = 999
	delete(snapshot, 1)
	snapshot[99] = []Record{{Value: "injected"}}

	actual := m.GetRecords(1)
	if len(actual) != 1 ||
		actual[0].Value != "original" ||
		actual[0].Clock["node-1"] != 1 {
		t.Fatalf("snapshot mutation changed store: %+v", actual)
	}

	if len(m.GetRecords(99)) != 0 {
		t.Fatal("snapshot insertion changed store")
	}
}

func TestSnapshotRemainsStableAfterWrites(t *testing.T) {
	m := newTestMap(t)

	if err := m.ApplyRecord(1, Record{
		Value: "before",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	snapshot := m.Snapshot()

	if err := m.ApplyRecord(1, Record{
		Value: "after",
		Clock: version.Clock{"node-1": 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := m.Put(2, "new key"); err != nil {
		t.Fatal(err)
	}

	if len(snapshot) != 1 ||
		len(snapshot[1]) != 1 ||
		snapshot[1][0].Value != "before" ||
		snapshot[1][0].Clock["node-1"] != 1 {
		t.Fatalf("later writes changed snapshot: %+v", snapshot)
	}
}

func TestSnapshotEmptyStore(t *testing.T) {
	m := newTestMap(t)

	if got := m.Snapshot(); len(got) != 0 {
		t.Fatalf("expected empty snapshot; got %+v", got)
	}
}
