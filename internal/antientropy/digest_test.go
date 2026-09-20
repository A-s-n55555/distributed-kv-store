package antientropy

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestRecordDigestIgnoresClockInsertionOrder(t *testing.T) {
	firstClock := make(version.Clock)
	firstClock["node-1"] = 3
	firstClock["node-2"] = 5

	secondClock := make(version.Clock)
	secondClock["node-2"] = 5
	secondClock["node-1"] = 3

	first := store.Record{Value: "hello", Clock: firstClock}
	second := store.Record{Value: "hello", Clock: secondClock}

	if RecordDigest(first) != RecordDigest(second) {
		t.Fatal("clock insertion order changed digest")
	}
}

func TestRecordDigestDetectsStateChanges(t *testing.T) {
	original := store.Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}

	changed := []store.Record{
		{
			Value: "different",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "hello",
			Clock: version.Clock{"node-1": 2},
		},
		{
			Value: "hello",
			Clock: version.Clock{"node-2": 1},
		},
	}

	for i, record := range changed {
		if RecordDigest(original) == RecordDigest(record) {
			t.Fatalf("change %d was not detected", i)
		}
	}

	// An empty live value and a tombstone are different states.
	live := store.Record{
		Clock: version.Clock{"node-1": 1},
	}
	deleted := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 1},
	}

	if RecordDigest(live) == RecordDigest(deleted) {
		t.Fatal("tombstone flag was not included")
	}
}

func TestRecordDigestNormalizesEmptyClock(t *testing.T) {
	first := store.Record{Value: "legacy"}
	second := store.Record{
		Value: "legacy",
		Clock: version.Clock{},
	}

	if RecordDigest(first) != RecordDigest(second) {
		t.Fatal("nil and empty clocks should hash equally")
	}
}

func TestKeyDigestIgnoresSiblingOrderAndDuplicates(t *testing.T) {
	first := store.Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}
	second := store.Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}

	expected := KeyDigest(10, []store.Record{first, second})

	if got := KeyDigest(10, []store.Record{second, first}); got != expected {
		t.Fatal("sibling order changed digest")
	}

	if got := KeyDigest(
		10,
		[]store.Record{first, second, first},
	); got != expected {
		t.Fatal("identical duplicate changed digest")
	}
}

func TestKeyDigestDetectsKeyAndSiblingChanges(t *testing.T) {
	first := store.Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}
	second := store.Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}

	records := []store.Record{first, second}
	expected := KeyDigest(10, records)

	if expected == KeyDigest(11, records) {
		t.Fatal("key change was not detected")
	}

	if expected == KeyDigest(10, []store.Record{first}) {
		t.Fatal("missing sibling was not detected")
	}

	if KeyDigest(10, nil) == KeyDigest(10, []store.Record{
		{
			Deleted: true,
			Clock:   version.Clock{"node-1": 1},
		},
	}) {
		t.Fatal("absence and tombstone must differ")
	}
}
