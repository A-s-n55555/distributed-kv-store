package server

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

// collectJoinKeys gathers keys visible on either old owner.
// This first implementation supports exactly two old nodes with N=2.
func (s *GRPCServer) collectJoinKeys(ctx context.Context) ([]int64, error) {
	if s.ring == nil || s.store == nil {
		return nil, fmt.Errorf("ring and store are required")
	}

	_, nodes := s.ring.Configuration()
	if len(nodes) != 2 || s.replicationFactor != 2 {
		return nil, fmt.Errorf(
			"join inventory currently requires two old nodes and N=2",
		)
	}

	var peer ring.Node
	foundLocal := false
	for _, node := range nodes {
		if node.ID == s.nodeID {
			foundLocal = true
		} else {
			peer = node
		}
	}
	if !foundLocal {
		return nil, fmt.Errorf("local node %q is absent from the ring", s.nodeID)
	}

	keys := make(map[int64]struct{})
	for key, records := range s.store.Snapshot() {
		if len(records) > 0 {
			keys[key] = struct{}{}
		}
	}

	configuration, err := s.antiEntropyConfigurationDigest()
	if err != nil {
		return nil, err
	}
	summary, err := s.fetchAntiEntropySummary(ctx, peer, configuration)
	if err != nil {
		return nil, fmt.Errorf("read inventory summary from %s: %w",
			peer.ID, err)
	}

	// Fetch buckets containing peer data, including data absent locally.
	emptyBuckets := antientropy.BuildTree(nil).BucketDigests()
	for bucket, emptyDigest := range emptyBuckets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if bytes.Equal(
			summary.GetBucketDigests()[bucket], emptyDigest[:],
		) {
			continue
		}

		response, err := s.fetchAntiEntropyBucket(
			ctx, peer, configuration, bucket,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"read inventory bucket %d from %s: %w",
				bucket, peer.ID, err,
			)
		}
		for _, state := range response.GetKeys() {
			if state == nil ||
				antientropy.BucketForKey(state.GetKey()) != bucket ||
				len(state.GetRecords()) == 0 {
				return nil, fmt.Errorf(
					"invalid key state in peer bucket %d", bucket,
				)
			}
			keys[state.GetKey()] = struct{}{}
		}
	}

	result := make([]int64, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result, nil
}
