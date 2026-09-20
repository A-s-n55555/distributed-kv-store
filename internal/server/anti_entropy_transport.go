package server

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const antiEntropyRPCTimeout = 2 * time.Second

// antiEntropyConfigurationDigest returns the fingerprint for the current
// static ring membership and replication configuration.
func (s *GRPCServer) antiEntropyConfigurationDigest() (
	antientropy.Digest,
	error,
) {
	if s.ring == nil {
		return antientropy.Digest{}, status.Error(
			codes.FailedPrecondition,
			"cluster ring is required",
		)
	}

	virtualNodes, nodes := s.ring.Configuration()

	digest, err := antientropy.ConfigurationDigest(
		virtualNodes,
		s.replicationFactor,
		nodes,
	)
	if err != nil {
		return antientropy.Digest{}, status.Errorf(
			codes.FailedPrecondition,
			"invalid cluster configuration: %v",
			err,
		)
	}

	return digest, nil
}

// fetchAntiEntropySummary requests the shared-data Merkle summary from peer.
func (s *GRPCServer) fetchAntiEntropySummary(
	ctx context.Context,
	peer ring.Node,
	configuration antientropy.Digest,
) (*kvpb.AntiEntropySummaryResponse, error) {
	if peer.ID == "" || peer.Address == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"peer ID and address are required",
		)
	}

	if peer.ID == s.nodeID {
		return nil, status.Error(
			codes.InvalidArgument,
			"cannot request anti-entropy summary from self",
		)
	}

	connection, err := grpc.NewClient(
		peer.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Unavailable,
			"connect to peer %s failed: %v",
			peer.ID,
			err,
		)
	}
	defer connection.Close()

	requestContext, cancel := context.WithTimeout(
		ctx,
		antiEntropyRPCTimeout,
	)
	defer cancel()

	response, err := kvpb.NewKeyValueStoreClient(connection).
		AntiEntropySummary(
			requestContext,
			&kvpb.AntiEntropySummaryRequest{
				RequesterNodeId:     s.nodeID,
				ReplicationFactor:   int32(s.replicationFactor),
				ConfigurationDigest: append([]byte(nil), configuration[:]...),
			},
		)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, status.Error(
			codes.Internal,
			"peer returned a nil anti-entropy summary",
		)
	}

	if response.GetResponderNodeId() != peer.ID {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"summary responder is %q; expected %q",
			response.GetResponderNodeId(),
			peer.ID,
		)
	}

	if !bytes.Equal(response.GetConfigurationDigest(), configuration[:]) {
		return nil, status.Error(
			codes.FailedPrecondition,
			"peer summary has a different configuration digest",
		)
	}

	if len(response.GetRootDigest()) != len(configuration) {
		return nil, status.Error(
			codes.Internal,
			"peer summary has an invalid root digest",
		)
	}

	if len(response.GetBucketDigests()) != antientropy.BucketCount {
		return nil, status.Errorf(
			codes.Internal,
			"peer summary has %d bucket digests; want %d",
			len(response.GetBucketDigests()),
			antientropy.BucketCount,
		)
	}

	for bucket, digest := range response.GetBucketDigests() {
		if len(digest) != len(configuration) {
			return nil, status.Errorf(
				codes.Internal,
				"peer summary bucket %d has an invalid digest",
				bucket,
			)
		}
	}

	return response, nil
}

// fetchAntiEntropyBucket requests every version for one shared-data bucket.
func (s *GRPCServer) fetchAntiEntropyBucket(
	ctx context.Context,
	peer ring.Node,
	configuration antientropy.Digest,
	bucket int,
) (*kvpb.AntiEntropyBucketResponse, error) {
	if bucket < 0 || bucket >= antientropy.BucketCount {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"bucket %d is outside valid range",
			bucket,
		)
	}

	connection, err := grpc.NewClient(
		peer.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Unavailable,
			"connect to peer %s failed: %v",
			peer.ID,
			err,
		)
	}
	defer connection.Close()

	requestContext, cancel := context.WithTimeout(
		ctx,
		antiEntropyRPCTimeout,
	)
	defer cancel()

	response, err := kvpb.NewKeyValueStoreClient(connection).
		AntiEntropyBucket(
			requestContext,
			&kvpb.AntiEntropyBucketRequest{
				RequesterNodeId:     s.nodeID,
				ReplicationFactor:   int32(s.replicationFactor),
				ConfigurationDigest: append([]byte(nil), configuration[:]...),
				Bucket:              int32(bucket),
			},
		)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, status.Error(
			codes.Internal,
			"peer returned a nil anti-entropy bucket",
		)
	}

	if response.GetResponderNodeId() != peer.ID {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"bucket responder is %q; expected %q",
			response.GetResponderNodeId(),
			peer.ID,
		)
	}

	if !bytes.Equal(response.GetConfigurationDigest(), configuration[:]) {
		return nil, status.Error(
			codes.FailedPrecondition,
			"peer bucket has a different configuration digest",
		)
	}

	if int(response.GetBucket()) != bucket {
		return nil, fmt.Errorf(
			"peer returned bucket %d; requested bucket %d",
			response.GetBucket(),
			bucket,
		)
	}

	return response, nil
}
