package server

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMissingReplicaVersions(t *testing.T) {
	first := store.Record{
		Value: "first",
		Clock: version.Clock{"node-1": 1},
	}

	second := store.Record{
		Value: "second",
		Clock: version.Clock{"node-2": 1},
	}

	global := []store.Record{first, second}

	tests := []struct {
		name      string
		local     []store.Record
		wantCount int
		wantValue string
	}{
		{
			name:      "replica has first sibling",
			local:     []store.Record{first},
			wantCount: 1,
			wantValue: "second",
		},
		{
			name:      "replica has second sibling",
			local:     []store.Record{second},
			wantCount: 1,
			wantValue: "first",
		},
		{
			name:      "replica has no versions",
			wantCount: 2,
		},
		{
			name:      "replica has all siblings",
			local:     global,
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			missing, err := missingReplicaVersions(
				global,
				tc.local,
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(missing) != tc.wantCount {
				t.Fatalf(
					"missing versions = %d; want %d: %+v",
					len(missing),
					tc.wantCount,
					missing,
				)
			}

			if tc.wantValue != "" &&
				missing[0].Value != tc.wantValue {
				t.Fatalf(
					"missing value = %q; want %q",
					missing[0].Value,
					tc.wantValue,
				)
			}
		})
	}
}

func TestMissingReplicaVersionsReturnsNewerVersion(t *testing.T) {
	newer := store.Record{
		Value: "new",
		Clock: version.Clock{"node-1": 2},
	}

	missing, err := missingReplicaVersions(
		[]store.Record{newer},
		[]store.Record{
			{
				Value: "old",
				Clock: version.Clock{"node-1": 1},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(missing) != 1 ||
		missing[0].Value != "new" {
		t.Fatalf("incorrect missing versions: %+v", missing)
	}
}

func TestMissingReplicaVersionsPreservesTombstone(t *testing.T) {
	tombstone := store.Record{
		Deleted: true,
		Clock:   version.Clock{"node-2": 3},
	}

	missing, err := missingReplicaVersions(
		[]store.Record{tombstone},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(missing) != 1 ||
		!missing[0].Deleted ||
		missing[0].Value != "" {
		t.Fatalf("incorrect missing tombstone: %+v", missing)
	}
}

func TestMissingReplicaVersionsRejectsContradiction(t *testing.T) {
	_, err := missingReplicaVersions(
		[]store.Record{
			{
				Value: "wanted",
				Clock: version.Clock{"node-1": 1},
			},
		},
		[]store.Record{
			{
				Value: "different",
				Clock: version.Clock{"node-1": 1},
			},
		},
	)

	if status.Code(err) != codes.Aborted {
		t.Fatalf("error = %v; want Aborted", err)
	}
}

func TestMissingReplicaVersionsCopiesClock(t *testing.T) {
	global := []store.Record{
		{
			Value: "value",
			Clock: version.Clock{"node-1": 1},
		},
	}

	missing, err := missingReplicaVersions(global, nil)
	if err != nil {
		t.Fatal(err)
	}

	missing[0].Clock["node-1"] = 999

	if global[0].Clock["node-1"] != 1 {
		t.Fatal("missing version exposed the input clock")
	}
}

