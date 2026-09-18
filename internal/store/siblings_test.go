package store

import (
	"errors"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestMergeRecordVersionsKeepsConcurrentSiblings(t *testing.T) {
	first := Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}

	second := Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}

	result, err := mergeRecordVersions([]Record{first}, second)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("siblings = %d; want 2", len(result))
	}
}

func TestMergeRecordVersionsRemovesDominatedSiblings(t *testing.T) {
	existing := []Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "second",
			Clock: version.Clock{"node-2": 1},
		},
	}

	resolved := Record{
		Value: "resolved",
		Clock: version.Clock{
			"node-1": 2,
			"node-2": 1,
		},
	}

	result, err := mergeRecordVersions(existing, resolved)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 || result[0].Value != "resolved" {
		t.Fatalf("incorrect result: %+v", result)
	}

	result[0].Clock["node-1"] = 999

	if resolved.Clock["node-1"] != 2 {
		t.Fatal("result shared the incoming clock")
	}
}

func TestMergeRecordVersionsDeduplicates(t *testing.T) {
	record := Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}

	result, err := mergeRecordVersions([]Record{record}, record)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("siblings = %d; want 1", len(result))
	}
}

func TestMergeRecordVersionsRejectsEqualClockMismatch(t *testing.T) {
	first := Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}

	second := Record{
		Value: "different",
		Clock: version.Clock{"node-1": 1},
	}

	_, err := mergeRecordVersions([]Record{first}, second)

	if !errors.Is(err, ErrRecordConflict) {
		t.Fatalf("error = %v; want ErrRecordConflict", err)
	}
}

func TestMergeRecordVersionsKeepsConcurrentTombstone(t *testing.T) {
	value := Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}

	tombstone := Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 1},
	}

	result, err := mergeRecordVersions([]Record{value}, tombstone)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("siblings = %d; want 2", len(result))
	}
}
