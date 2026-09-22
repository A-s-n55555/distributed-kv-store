package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const membershipStatusRPCTimeout = 2 * time.Second

// fetchMembershipStatus reads and validates one remote member's status.
func (s *GRPCServer) fetchMembershipStatus(
	ctx context.Context,
	peer ring.Node,
) (*kvpb.MembershipStatusResponse, error) {
	if peer.ID == "" || peer.Address == "" || peer.ID == s.nodeID {
		return nil, status.Error(codes.InvalidArgument, "valid remote peer is required")
	}

	connection, err := grpc.NewClient(
		peer.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect to %s: %v", peer.ID, err)
	}
	defer connection.Close()

	rpcContext, cancel := context.WithTimeout(ctx, membershipStatusRPCTimeout)
	defer cancel()

	response, err := kvpb.NewKeyValueStoreClient(connection).
		GetMembershipStatus(rpcContext, &kvpb.MembershipStatusRequest{})
	if err != nil {
		return nil, err
	}
	if response == nil ||
		response.GetNodeId() != peer.ID ||
		response.GetActiveEpoch() == 0 ||
		len(response.GetActiveDigest()) != sha256.Size {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"invalid membership status from %s", peer.ID,
		)
	}

	if response.GetPendingEpoch() == 0 {
		if len(response.GetPendingDigest()) != 0 {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"invalid pending identity from %s", peer.ID,
			)
		}
	} else if (!response.GetWritesPaused() && !response.GetClientsReleased()) ||
		len(response.GetPendingDigest()) != sha256.Size {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"invalid paused identity from %s", peer.ID,
		)
	}

	if response.GetClientsReleased() {
		if response.GetWritesPaused() ||
			!response.GetReplicasReady() ||
			response.GetPendingEpoch() != response.GetActiveEpoch() ||
			!bytes.Equal(
				response.GetPendingDigest(),
				response.GetActiveDigest(),
			) {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"invalid released membership status from %s", peer.ID,
			)
		}
	}
	return response, nil
}
