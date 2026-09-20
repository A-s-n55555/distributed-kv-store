package server

import (
	"bytes"
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// syncAntiEntropyFromPeer pulls every version missing from this node.
// It does not delete or overwrite local siblings. ApplyRecord keeps the
// causally newest versions and retains concurrent versions as siblings.
//
// Running this operation from every node provides two-way convergence.
func (s *GRPCServer) syncAntiEntropyFromPeer(
	ctx context.Context,
	peer ring.Node,
) (int, error) {
	if peer.ID == "" || peer.Address == "" {
		return 0, status.Error(
			codes.InvalidArgument,
			"peer ID and address are required",
		)
	}

	if peer.ID == s.nodeID {
		return 0, status.Error(
			codes.InvalidArgument,
			"cannot synchronize anti-entropy data from self",
		)
	}

	configuration, err := s.antiEntropyConfigurationDigest()
	if err != nil {
		return 0, err
	}

	localShared, err := antientropy.SharedSnapshot(
		s.store.Snapshot(),
		s.ring,
		s.replicationFactor,
		s.nodeID,
		peer.ID,
	)
	if err != nil {
		return 0, status.Errorf(
			codes.FailedPrecondition,
			"cannot select local shared records: %v",
			err,
		)
	}

	localTree := antientropy.BuildTree(localShared)

	peerSummary, err := s.fetchAntiEntropySummary(
		ctx,
		peer,
		configuration,
	)
	if err != nil {
		return 0, err
	}

	localRoot := localTree.Root()

	if bytes.Equal(peerSummary.GetRootDigest(), localRoot[:]) {
		return 0, nil
	}

	localBuckets := localTree.BucketDigests()
	updated := 0

	for bucket, localDigest := range localBuckets {
		if bytes.Equal(
			peerSummary.GetBucketDigests()[bucket],
			localDigest[:],
		) {
			continue
		}

		peerBucket, err := s.fetchAntiEntropyBucket(
			ctx,
			peer,
			configuration,
			bucket,
		)
		if err != nil {
			return updated, err
		}

		applied, err := s.applyAntiEntropyBucket(
			peerBucket,
			bucket,
		)
		if err != nil {
			return updated, err
		}

		updated += applied
	}

	return updated, nil
}

// applyAntiEntropyBucket validates and stores every version received from
// one peer bucket. Duplicate and stale records are safely ignored by ApplyRecord.
func (s *GRPCServer) applyAntiEntropyBucket(
	response *kvpb.AntiEntropyBucketResponse,
	bucket int,
) (int, error) {
	if response == nil {
		return 0, status.Error(
			codes.Internal,
			"anti-entropy peer returned a nil bucket response",
		)
	}

	seenKeys := make(map[int64]struct{})
	applied := 0

	for _, keyState := range response.GetKeys() {
		if keyState == nil {
			return applied, status.Error(
				codes.Internal,
				"anti-entropy peer returned a nil key state",
			)
		}

		key := keyState.GetKey()

		if antientropy.BucketForKey(key) != bucket {
			return applied, status.Errorf(
				codes.Internal,
				"key %d does not belong to bucket %d",
				key,
				bucket,
			)
		}

		if _, exists := seenKeys[key]; exists {
			return applied, status.Errorf(
				codes.Internal,
				"peer returned duplicate key %d in one bucket",
				key,
			)
		}
		seenKeys[key] = struct{}{}

		if len(keyState.GetRecords()) == 0 {
			return applied, status.Errorf(
				codes.Internal,
				"peer returned key %d without versions",
				key,
			)
		}

		for _, wireRecord := range keyState.GetRecords() {
			record, err := recordFromAntiEntropyProto(wireRecord)
			if err != nil {
				return applied, err
			}

			if err := s.store.ApplyRecord(key, record); err != nil {
				return applied, status.Errorf(
					codes.Aborted,
					"apply peer version for key %d failed: %v",
					key,
					err,
				)
			}

			applied++
		}
	}

	return applied, nil
}

// recordFromAntiEntropyProto validates and copies one received version.
func recordFromAntiEntropyProto(
	wireRecord *kvpb.VersionedRecord,
) (store.Record, error) {
	if wireRecord == nil {
		return store.Record{}, status.Error(
			codes.Internal,
			"anti-entropy peer returned a nil version",
		)
	}

	if wireRecord.GetDeleted() && wireRecord.GetValue() != "" {
		return store.Record{}, status.Error(
			codes.Internal,
			"anti-entropy peer returned a tombstone with a value",
		)
	}

	clock := make(version.Clock, len(wireRecord.GetClock()))

	for nodeID, counter := range wireRecord.GetClock() {
		if nodeID == "" || counter == 0 {
			return store.Record{}, status.Error(
				codes.Internal,
				"anti-entropy peer returned an invalid vector clock",
			)
		}

		clock[nodeID] = counter
	}

	return store.Record{
		Value:   wireRecord.GetValue(),
		Clock:   clock,
		Deleted: wireRecord.GetDeleted(),
	}, nil
}
