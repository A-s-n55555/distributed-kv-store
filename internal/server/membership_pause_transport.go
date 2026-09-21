package server

import (
	"bytes"
	"context"
	"net"
	"strings"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const membershipPauseRPCTimeout = 3 * time.Second

// pauseJoinPeer asks one old member to durably pause for this exact join.
func (s *GRPCServer) pauseJoinPeer(
	ctx context.Context,
	peer ring.Node,
	current membership.Configuration,
	candidate membership.Configuration,
) (*kvpb.MembershipStatusResponse, error) {
	if peer.ID == "" || peer.ID == s.nodeID {
		return nil, status.Error(codes.InvalidArgument, "a remote old member is required")
	}

	knownPeer := false
	for _, node := range current.Nodes() {
		if node.ID == peer.ID && node.Address == peer.Address {
			knownPeer = true
			break
		}
	}
	if !knownPeer {
		return nil, status.Error(codes.InvalidArgument, "peer is absent from active membership")
	}

	host, _, err := net.SplitHostPort(peer.Address)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid peer address: %v", err)
	}
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, status.Error(codes.InvalidArgument, "membership control requires a loopback address")
		}
	}

	joining, err := membership.ValidateJoinCandidate(current, candidate)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid join candidate: %v", err)
	}

	s.writeMu.Lock()
	localActive := s.activeMembership.Identity()
	token := s.membershipControlToken
	s.writeMu.Unlock()

	activeID := current.Identity()
	candidateID := candidate.Identity()
	if localActive != activeID {
		return nil, status.Error(codes.FailedPrecondition, "local active membership changed")
	}
	if token == "" {
		return nil, status.Error(codes.FailedPrecondition, "membership control token is not configured")
	}

	connection, err := grpc.NewClient(
		peer.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect to %s: %v", peer.ID, err)
	}
	defer connection.Close()

	rpcContext, cancel := context.WithTimeout(ctx, membershipPauseRPCTimeout)
	defer cancel()
	rpcContext = metadata.AppendToOutgoingContext(
		rpcContext, membershipControlTokenHeader, token,
	)

	response, err := kvpb.NewKeyValueStoreClient(connection).PrepareJoinPause(
		rpcContext,
		&kvpb.PrepareJoinPauseRequest{
			ActiveEpoch:        activeID.Epoch,
			ActiveDigest:       append([]byte(nil), activeID.Digest[:]...),
			CandidateEpoch:     candidateID.Epoch,
			CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
			JoiningNodeId:      joining.ID,
			JoiningNodeAddress: joining.Address,
		},
	)
	if err != nil {
		return nil, err
	}
	if response == nil ||
		response.GetNodeId() != peer.ID ||
		response.GetActiveEpoch() != activeID.Epoch ||
		!bytes.Equal(response.GetActiveDigest(), activeID.Digest[:]) ||
		!response.GetWritesPaused() ||
		response.GetPendingEpoch() != candidateID.Epoch ||
		!bytes.Equal(response.GetPendingDigest(), candidateID.Digest[:]) {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"peer %s did not confirm the requested join pause", peer.ID,
		)
	}

	return response, nil
}
