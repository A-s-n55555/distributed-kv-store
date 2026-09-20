package server

import (
	"bytes"
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// antiEntropySnapshot validates peer configuration and selects the
// locally stored keys that both nodes are assigned to replicate.
//
// Membership must remain static during this operation.
func (s *GRPCServer) antiEntropySnapshot(
	ctx context.Context,
	requesterID string,
	replicationFactor int32,
	configurationDigest []byte,
) (map[int64][]store.Record, antientropy.Digest, error) {
	var empty antientropy.Digest

	if err := ctx.Err(); err != nil {
		return nil, empty, status.FromContextError(err).Err()
	}

	if requesterID == "" || requesterID == s.nodeID {
		return nil, empty, status.Error(
			codes.InvalidArgument,
			"requester must identify a different cluster node",
		)
	}

	if s.ring == nil || s.store == nil {
		return nil, empty, status.Error(
			codes.FailedPrecondition,
			"server ring and store must be initialized",
		)
	}

	if len(configurationDigest) != len(empty) {
		return nil, empty, status.Error(
			codes.InvalidArgument,
			"configuration digest must contain 32 bytes",
		)
	}

	if int(replicationFactor) != s.replicationFactor {
		return nil, empty, status.Error(
			codes.FailedPrecondition,
			"replication factors do not match",
		)
	}

	virtualNodes, nodes := s.ring.Configuration()

	hasLocal := false
	hasRequester := false

	for _, node := range nodes {
		if node.ID == s.nodeID {
			hasLocal = true
		}
		if node.ID == requesterID {
			hasRequester = true
		}
	}

	if !hasLocal {
		return nil, empty, status.Error(
			codes.FailedPrecondition,
			"local node is absent from cluster membership",
		)
	}

	if !hasRequester {
		return nil, empty, status.Error(
			codes.InvalidArgument,
			"requester is absent from cluster membership",
		)
	}

	digest, err := antientropy.ConfigurationDigest(
		virtualNodes,
		s.replicationFactor,
		nodes,
	)
	if err != nil {
		return nil, empty, status.Errorf(
			codes.FailedPrecondition,
			"invalid cluster configuration: %v",
			err,
		)
	}

	if !bytes.Equal(configurationDigest, digest[:]) {
		return nil, empty, status.Error(
			codes.FailedPrecondition,
			"cluster configuration digests do not match",
		)
	}

	shared, err := antientropy.SharedSnapshot(
		s.store.Snapshot(),
		s.ring,
		s.replicationFactor,
		s.nodeID,
		requesterID,
	)
	if err != nil {
		return nil, empty, status.Errorf(
			codes.FailedPrecondition,
			"cannot select shared records: %v",
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return nil, empty, status.FromContextError(err).Err()
	}

	return shared, digest, nil
}

// AntiEntropySummary returns hashes for data shared with the requester.
func (s *GRPCServer) AntiEntropySummary(
	ctx context.Context,
	request *kvpb.AntiEntropySummaryRequest,
) (*kvpb.AntiEntropySummaryResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	shared, configuration, err := s.antiEntropySnapshot(
		ctx,
		request.GetRequesterNodeId(),
		request.GetReplicationFactor(),
		request.GetConfigurationDigest(),
	)
	if err != nil {
		return nil, err
	}

	tree := antientropy.BuildTree(shared)
	root := tree.Root()
	buckets := tree.BucketDigests()

	wireBuckets := make([][]byte, len(buckets))
	for i := range buckets {
		wireBuckets[i] = append([]byte(nil), buckets[i][:]...)
	}

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	return &kvpb.AntiEntropySummaryResponse{
		ResponderNodeId:     s.nodeID,
		ConfigurationDigest: append([]byte(nil), configuration[:]...),
		RootDigest:          append([]byte(nil), root[:]...),
		BucketDigests:       wireBuckets,
	}, nil
}

// AntiEntropyBucket returns complete sibling sets for one shared bucket.
func (s *GRPCServer) AntiEntropyBucket(
	ctx context.Context,
	request *kvpb.AntiEntropyBucketRequest,
) (*kvpb.AntiEntropyBucketResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	bucket := int(request.GetBucket())
	if bucket < 0 || bucket >= antientropy.BucketCount {
		return nil, status.Error(
			codes.InvalidArgument,
			"bucket must be between 0 and 255",
		)
	}

	shared, configuration, err := s.antiEntropySnapshot(
		ctx,
		request.GetRequesterNodeId(),
		request.GetReplicationFactor(),
		request.GetConfigurationDigest(),
	)
	if err != nil {
		return nil, err
	}

	states, err := antientropy.BucketSnapshot(shared, bucket)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"cannot extract bucket: %v",
			err,
		)
	}

	keys := make([]*kvpb.AntiEntropyKeyState, 0, len(states))

	for _, state := range states {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		wireRecords := make([]*kvpb.VersionedRecord, 0, len(state.Records))
		for _, record := range state.Records {
			wireRecords = append(wireRecords, recordToProto(record))
		}

		keys = append(keys, &kvpb.AntiEntropyKeyState{
			Key:     state.Key,
			Records: wireRecords,
		})
	}

	return &kvpb.AntiEntropyBucketResponse{
		ResponderNodeId:     s.nodeID,
		ConfigurationDigest: append([]byte(nil), configuration[:]...),
		Bucket:              request.GetBucket(),
		Keys:                keys,
	}, nil
}
