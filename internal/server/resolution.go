package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkedResolutionContext validates explicit client intent.
// Exact equality rejects both stale and ahead-of-observed context.
func checkedResolutionContext(
	supplied version.Clock,
	records []store.Record,
) (version.Clock, error) {
	if len(supplied) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"resolution context is required",
		)
	}

	for nodeID, counter := range supplied {
		if nodeID == "" || counter == 0 {
			return nil, status.Error(
				codes.InvalidArgument,
				"invalid resolution context",
			)
		}
	}

	if len(records) == 0 {
		return nil, status.Error(
			codes.FailedPrecondition,
			"no observed versions to resolve",
		)
	}

	observed := make(version.Clock)

	for _, record := range records {
		for nodeID, counter := range record.Clock {
			if counter > observed[nodeID] {
				observed[nodeID] = counter
			}
		}
	}

	if version.Compare(supplied, observed) != version.Equal {
		return nil, status.Error(
			codes.Aborted,
			"resolution context does not match observed versions; Get again",
		)
	}

	return version.Clone(supplied), nil
}

func (s *GRPCServer) Resolve(
	ctx context.Context,
	request *kvpb.ResolveRequest,
) (*kvpb.ResolveResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	// Copy protobuf-owned context before using it.
	causalContext := version.Clone(
		version.Clock(request.GetContext()),
	)

	if err := s.coordinateWrite(
		ctx,
		request.GetKey(),
		request.GetValue(),
		request.GetDeleted(),
		causalContext,
		true,
	); err != nil {
		return nil, err
	}

	return &kvpb.ResolveResponse{}, nil
}
