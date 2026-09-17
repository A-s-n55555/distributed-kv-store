package server

import (
	"context"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestNeedsReadRepair(t *testing.T) {
	selected := store.Record{
		Value: "new",
		Clock: version.Clock{"node-1": 2},
	}

	tests := []struct {
		name     string
		observed store.Record
		exists   bool
		want     bool
	}{
		{
			name:   "missing replica",
			exists: false,
			want:   true,
		},
		{
			name: "older replica",
			observed: store.Record{
				Value: "old",
				Clock: version.Clock{"node-1": 1},
			},
			exists: true,
			want:   true,
		},
		{
			name:     "equal replica",
			observed: selected,
			exists:   true,
			want:     false,
		},
		{
			name: "newer replica",
			observed: store.Record{
				Value: "newer",
				Clock: version.Clock{"node-1": 3},
			},
			exists: true,
			want:   false,
		},
		{
			name: "concurrent replica",
			observed: store.Record{
				Value: "other",
				Clock: version.Clock{"node-2": 1},
			},
			exists: true,
			want:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := needsReadRepair(
				selected,
				test.observed,
				test.exists,
			)

			if got != test.want {
				t.Fatalf("needsReadRepair() = %v; want %v", got, test.want)
			}
		})
	}

	if needsReadRepair(store.Record{Value: "legacy"}, store.Record{}, false) {
		t.Fatal("unversioned record was eligible for repair")
	}
}

func TestReadRepairAppliesTombstone(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"

	old := store.Record{
		Value: "old",
		Clock: version.Clock{"node-1": 1},
	}

	if err := s.store.ApplyRecord(1, old); err != nil {
		t.Fatal(err)
	}

	selected := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-1": 2},
	}

	repairErrors := s.repairObservedReplicas(
		context.Background(),
		1,
		selected,
		[]replicaObservation{
			{
				node:   ring.Node{ID: "node-1"},
				record: old,
				exists: true,
			},
		},
	)

	if len(repairErrors) != 0 {
		t.Fatalf("repair errors = %v", repairErrors)
	}

	record, exists := s.store.GetRecord(1)
	if !exists ||
		!record.Deleted ||
		record.Clock["node-1"] != 2 {
		t.Fatalf("incorrect repaired record: %+v", record)
	}

	if _, found := s.store.Get(1); found {
		t.Fatal("repaired tombstone was exposed as a live value")
	}
}

func TestReadRepairCreatesMissingCopy(t *testing.T) {
	s := newRecordTestServer(t)
	s.nodeID = "node-1"

	selected := store.Record{
		Value: "repaired",
		Clock: version.Clock{"node-2": 3},
	}

	repairErrors := s.repairObservedReplicas(
		context.Background(),
		1,
		selected,
		[]replicaObservation{
			{
				node:   ring.Node{ID: "node-1"},
				exists: false,
			},
		},
	)

	if len(repairErrors) != 0 {
		t.Fatalf("repair errors = %v", repairErrors)
	}

	record, exists := s.store.GetRecord(1)
	if !exists ||
		record.Value != "repaired" ||
		record.Clock["node-2"] != 3 {
		t.Fatalf("incorrect repaired copy: %+v", record)
	}
}
