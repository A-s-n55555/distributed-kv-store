package server

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSelectNewestRecord(t *testing.T) {
	tests := []struct {
		name        string
		records     []store.Record
		wantExists  bool
		wantValue   string
		wantDeleted bool
		wantCode    codes.Code
	}{
		{
			name:       "no records",
			wantExists: false,
			wantCode:   codes.OK,
		},
		{
			name: "select newer value",
			records: []store.Record{
				{
					Value: "old",
					Clock: version.Clock{"node-1": 1},
				},
				{
					Value: "new",
					Clock: version.Clock{"node-1": 2},
				},
			},
			wantExists: true,
			wantValue:  "new",
			wantCode:   codes.OK,
		},
		{
			name: "newer tombstone wins",
			records: []store.Record{
				{
					Value: "old",
					Clock: version.Clock{"node-1": 1},
				},
				{
					Deleted: true,
					Clock:   version.Clock{"node-1": 2},
				},
			},
			wantExists:  true,
			wantDeleted: true,
			wantCode:    codes.OK,
		},
		{
			name: "concurrent versions",
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
			wantCode: codes.Aborted,
		},
		{
			name: "equal clock different contents",
			records: []store.Record{
				{
					Value: "first",
					Clock: version.Clock{"node-1": 1},
				},
				{
					Value: "second",
					Clock: version.Clock{"node-1": 1},
				},
			},
			wantCode: codes.Aborted,
		},
		{
			name: "later record dominates concurrent predecessors",
			records: []store.Record{
				{
					Value: "first",
					Clock: version.Clock{"node-1": 1},
				},
				{
					Value: "second",
					Clock: version.Clock{"node-2": 1},
				},
				{
					Value: "merged",
					Clock: version.Clock{
						"node-1": 2,
						"node-2": 1,
					},
				},
			},
			wantExists: true,
			wantValue:  "merged",
			wantCode:   codes.OK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, exists, err := selectNewestRecord(test.records)

			if status.Code(err) != test.wantCode {
				t.Fatalf(
					"code = %v; want %v; error = %v",
					status.Code(err),
					test.wantCode,
					err,
				)
			}

			if err != nil {
				return
			}

			if exists != test.wantExists ||
				record.Value != test.wantValue ||
				record.Deleted != test.wantDeleted {
				t.Fatalf(
					"record=%+v exists=%v; unexpected result",
					record,
					exists,
				)
			}
		})
	}
}
