package server

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
)

func TestBuildVersionedGetResponse(t *testing.T) {
	tests := []struct {
		name     string
		records  []store.Record
		found    bool
		conflict bool
		value    string
	}{
		{
			name: "missing",
		},
		{
			name: "single live version",
			records: []store.Record{
				{
					Value: "hello",
					Clock: version.Clock{"node-1": 1},
				},
			},
			found: true,
			value: "hello",
		},
		{
			name: "tombstone",
			records: []store.Record{
				{
					Deleted: true,
					Clock:   version.Clock{"node-1": 2},
				},
			},
		},
		{
			name: "concurrent live versions",
			records: []store.Record{
				{
					Value: "first",
					Clock: version.Clock{"node-1": 1},
				},
				{
					Value: "second",
					Clock: version.Clock{"node-2": 1},
				},
			},
			found:    true,
			conflict: true,
		},
		{
			name: "concurrent live and delete",
			records: []store.Record{
				{
					Value: "live",
					Clock: version.Clock{"node-1": 1},
				},
				{
					Deleted: true,
					Clock:   version.Clock{"node-2": 1},
				},
			},
			found:    true,
			conflict: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response := buildVersionedGetResponse(tc.records)

			if response.Found != tc.found ||
				response.Conflict != tc.conflict ||
				response.Value != tc.value {
				t.Fatalf("unexpected response: %+v", response)
			}

			if len(response.Versions) != len(tc.records) {
				t.Fatalf(
					"versions = %d; want %d",
					len(response.Versions),
					len(tc.records),
				)
			}

			expectedContext := make(map[string]uint64)
			for _, record := range tc.records {
				for nodeID, counter := range record.Clock {
					if counter > expectedContext[nodeID] {
						expectedContext[nodeID] = counter
					}
				}
			}

			if len(response.Context) != len(expectedContext) {
				t.Fatalf("incorrect context: %v", response.Context)
			}

			for nodeID, counter := range expectedContext {
				if response.Context[nodeID] != counter {
					t.Fatalf("incorrect context: %v", response.Context)
				}
			}
		})
	}
}
