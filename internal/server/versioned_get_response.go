package server

import (
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
)

func buildVersionedGetResponse(
	versions []store.Record,
) *kvpb.GetResponse {
	response := &kvpb.GetResponse{
		Context:  make(map[string]uint64),
		Conflict: len(versions) > 1,
	}

	for _, record := range versions {
		response.Versions = append(
			response.Versions,
			recordToProto(record),
		)

		for nodeID, counter := range record.Clock {
			if counter > response.Context[nodeID] {
				response.Context[nodeID] = counter
			}
		}

		// Found means at least one retained version is live.
		if !record.Deleted {
			response.Found = true
		}
	}

	// Never put an arbitrary sibling into the legacy Value field.
	if len(versions) == 1 && !versions[0].Deleted {
		response.Value = versions[0].Value
	}

	return response
}
