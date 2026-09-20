package server

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reuse the existing store/WAL test setup, then configure a two-node ring.
// These tests call handlers directly; no running servers are required.
func newAntiEntropyTestServer(
	t *testing.T,
) (*GRPCServer, []byte) {
	t.Helper()

	s := newRecordTestServer(t)
	s.nodeID = "node-1"
	s.replicationFactor = 2
	s.ring = ring.New(32)

	s.ring.AddNode(ring.Node{
		ID: "node-1", Address: "localhost:50051",
	})
	s.ring.AddNode(ring.Node{
		ID: "node-2", Address: "localhost:50052",
	})

	virtualNodes, nodes := s.ring.Configuration()
	digest, err := antientropy.ConfigurationDigest(
		virtualNodes, s.replicationFactor, nodes,
	)
	if err != nil {
		t.Fatal(err)
	}

	return s, append([]byte(nil), digest[:]...)
}

func TestAntiEntropySummaryMatchesSnapshot(t *testing.T) {
	s, configuration := newAntiEntropyTestServer(t)

	if err := s.store.ApplyRecord(42, store.Record{
		Value: "hello",
		Clock: version.Clock{"node-1": 1},
	}); err != nil {
		t.Fatal(err)
	}

	response, err := s.AntiEntropySummary(
		context.Background(),
		&kvpb.AntiEntropySummaryRequest{
			RequesterNodeId:     "node-2",
			ReplicationFactor:   2,
			ConfigurationDigest: configuration,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if response.GetResponderNodeId() != "node-1" {
		t.Fatal("unexpected responder")
	}
	if !bytes.Equal(response.GetConfigurationDigest(), configuration) {
		t.Fatal("configuration digest mismatch")
	}

	// With two nodes and RF=2, every stored key belongs to both nodes.
	tree := antientropy.BuildTree(s.store.Snapshot())
	root := tree.Root()

	if !bytes.Equal(response.GetRootDigest(), root[:]) {
		t.Fatal("root digest does not match stored data")
	}

	expected := tree.BucketDigests()
	actual := response.GetBucketDigests()

	if len(actual) != antientropy.BucketCount {
		t.Fatalf("got %d bucket digests; want 256", len(actual))
	}
	for i := range expected {
		if !bytes.Equal(actual[i], expected[i][:]) {
			t.Fatalf("bucket %d digest mismatch", i)
		}
	}
}

func TestAntiEntropyBucketPreservesSiblingsAndTombstone(t *testing.T) {
	s, configuration := newAntiEntropyTestServer(t)

	const key int64 = 42
	records := []store.Record{
		{
			Value: "live-sibling",
			Clock: version.Clock{"node-1": 1},
		},
		{
			Deleted: true,
			Clock:   version.Clock{"node-2": 1},
		},
	}

	for _, record := range records {
		if err := s.store.ApplyRecord(key, record); err != nil {
			t.Fatal(err)
		}
	}

	bucket := antientropy.BucketForKey(key)
	response, err := s.AntiEntropyBucket(
		context.Background(),
		&kvpb.AntiEntropyBucketRequest{
			RequesterNodeId:     "node-2",
			ReplicationFactor:   2,
			ConfigurationDigest: configuration,
			Bucket:              int32(bucket),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if response.GetResponderNodeId() != "node-1" ||
		!bytes.Equal(response.GetConfigurationDigest(), configuration) ||
		response.GetBucket() != int32(bucket) {
		t.Fatal("incorrect bucket response metadata")
	}

	keys := response.GetKeys()
	if len(keys) != 1 || keys[0].GetKey() != key {
		t.Fatalf("unexpected key states: %v", keys)
	}

	wireRecords := keys[0].GetRecords()
	if len(wireRecords) != len(records) {
		t.Fatalf("got %d versions; want 2", len(wireRecords))
	}

	// Compare contents without assuming sibling order.
	for _, expected := range records {
		found := false
		for _, wireRecord := range wireRecords {
			if wireRecord != nil &&
				reflect.DeepEqual(recordFromProto(wireRecord), expected) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing version: %+v", expected)
		}
	}
}

func TestAntiEntropyRejectsInvalidRequests(t *testing.T) {
	s, configuration := newAntiEntropyTestServer(t)

	wrongDigest := append([]byte(nil), configuration...)
	wrongDigest[0] ^= 0xff

	tests := []struct {
		name   string
		peer   string
		factor int32
		digest []byte
		want   codes.Code
	}{
		{"missing peer", "", 2, configuration, codes.InvalidArgument},
		{"self", "node-1", 2, configuration, codes.InvalidArgument},
		{"unknown peer", "node-3", 2, configuration, codes.InvalidArgument},
		{"wrong factor", "node-2", 1, configuration, codes.FailedPrecondition},
		{"short digest", "node-2", 2, []byte{1}, codes.InvalidArgument},
		{"wrong digest", "node-2", 2, wrongDigest, codes.FailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.AntiEntropySummary(
				context.Background(),
				&kvpb.AntiEntropySummaryRequest{
					RequesterNodeId:     tt.peer,
					ReplicationFactor:   tt.factor,
					ConfigurationDigest: tt.digest,
				},
			)
			if status.Code(err) != tt.want {
				t.Fatalf("summary error = %v; want %v", err, tt.want)
			}

			_, err = s.AntiEntropyBucket(
				context.Background(),
				&kvpb.AntiEntropyBucketRequest{
					RequesterNodeId:     tt.peer,
					ReplicationFactor:   tt.factor,
					ConfigurationDigest: tt.digest,
					Bucket:              0,
				},
			)
			if status.Code(err) != tt.want {
				t.Fatalf("bucket error = %v; want %v", err, tt.want)
			}
		})
	}

	for _, bucket := range []int32{-1, int32(antientropy.BucketCount)} {
		_, err := s.AntiEntropyBucket(
			context.Background(),
			&kvpb.AntiEntropyBucketRequest{
				RequesterNodeId:     "node-2",
				ReplicationFactor:   2,
				ConfigurationDigest: configuration,
				Bucket:              bucket,
			},
		)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("bucket %d: unexpected error: %v", bucket, err)
		}
	}
}
