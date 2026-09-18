package server

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckedResolutionContext(t *testing.T) {
	records := []store.Record{
		{
			Value: "first",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Value: "second",
			Clock: version.Clock{"node-2": 1},
		},
	}

	tests := []struct {
		name    string
		context version.Clock
		code    codes.Code
	}{
		{
			name: "complete",
			context: version.Clock{
				"node-1": 1,
				"node-2": 1,
			},
			code: codes.OK,
		},
		{
			name: "missing context",
			code: codes.InvalidArgument,
		},
		{
			name:    "missing sibling",
			context: version.Clock{"node-1": 1},
			code:    codes.Aborted,
		},
		{
			name: "ahead of observed",
			context: version.Clock{
				"node-1": 2,
				"node-2": 1,
			},
			code: codes.Aborted,
		},
		{
			name:    "zero counter",
			context: version.Clock{"node-1": 0},
			code:    codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := checkedResolutionContext(
				tc.context,
				records,
			)

			if status.Code(err) != tc.code {
				t.Fatalf(
					"code = %v; want %v; error = %v",
					status.Code(err),
					tc.code,
					err,
				)
			}

			if err == nil {
				got["node-1"] = 999
				if tc.context["node-1"] != 1 {
					t.Fatal("returned context aliases input")
				}
			}
		})
	}
}

func TestResolutionRequiresObservedVersions(t *testing.T) {
	_, err := checkedResolutionContext(
		version.Clock{"node-1": 1},
		nil,
	)

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("error = %v; want FailedPrecondition", err)
	}
}
