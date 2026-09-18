package server

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMergeReplicaVersionsPreservesConcurrency(t *testing.T) {
	input := []store.Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "second",
			Clock: version.Clock{"node-2": 1},
		},
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
	}

	got, err := mergeReplicaVersions(input)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("siblings = %d; want 2", len(got))
	}

	got[0].Clock["node-1"] = 999
	if input[0].Clock["node-1"] != 1 {
		t.Fatal("merge exposed an input clock")
	}
}

func TestMergeReplicaVersionsPrunesSupersededRecords(t *testing.T) {
	got, err := mergeReplicaVersions([]store.Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "second",
			Clock: version.Clock{"node-2": 1},
		},
		{
			Value: "resolved",
			Clock: version.Clock{
				"node-1": 2,
				"node-2": 1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0].Value != "resolved" {
		t.Fatalf("unexpected versions: %+v", got)
	}
}

func TestMergeReplicaVersionsRejectsContradiction(t *testing.T) {
	_, err := mergeReplicaVersions([]store.Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "different",
			Clock: version.Clock{"node-1": 1},
		},
	})

	if status.Code(err) != codes.Aborted {
		t.Fatalf("error = %v; want Aborted", err)
	}
}

func TestMergeReplicaVersionsPreservesConcurrentDelete(t *testing.T) {
	got, err := mergeReplicaVersions([]store.Record{
		{
			Value: "live",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Deleted: true,
			Clock:   version.Clock{"node-2": 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("siblings = %d; want 2", len(got))
	}

	deleted := 0
	for _, record := range got {
		if record.Deleted {
			deleted++
		}
	}

	if deleted != 1 {
		t.Fatalf("tombstones = %d; want 1", deleted)
	}
}

func TestMergeReplicaVersionsEmpty(t *testing.T) {
	got, err := mergeReplicaVersions(nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %+v, error %v; want no versions", got, err)
	}
}
