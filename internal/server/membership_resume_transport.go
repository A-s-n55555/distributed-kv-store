package server

import (
	"bytes"
	"context"
	"net"
	"strings"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// requireJoinResumeDecision verifies the original coordinator's decision.
// Called only after the incoming control request has been authorized.
func (s *GRPCServer) requireJoinResumeAuthorization(
	ctx context.Context,
	previous, candidate membership.Configuration,
	request *kvpb.PrepareJoinPauseRequest,
	requireClientRelease bool,
) error {
	coordinator, err := membership.JoinCoordinator(previous)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}

	var response *kvpb.JoinResumeDecisionResponse

	if coordinator.ID == s.nodeID {
		response, err = s.GetJoinResumeDecision(ctx, request)
	} else {
		host, _, addressErr := net.SplitHostPort(coordinator.Address)
		if addressErr != nil {
			return status.Errorf(
				codes.FailedPrecondition,
				"invalid coordinator address: %v", addressErr,
			)
		}
		ip := net.ParseIP(host)
		if !strings.EqualFold(host, "localhost") &&
			(ip == nil || !ip.IsLoopback()) {
			return status.Error(
				codes.FailedPrecondition,
				"membership control requires a loopback address",
			)
		}

		s.writeMu.Lock()
		token := s.membershipControlToken
		s.writeMu.Unlock()
		if token == "" {
			return status.Error(
				codes.FailedPrecondition,
				"membership control token is not configured",
			)
		}

		connection, connectErr := grpc.NewClient(
			coordinator.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if connectErr != nil {
			return status.Errorf(
				codes.Unavailable, "connect to coordinator: %v", connectErr,
			)
		}
		defer connection.Close()

		rpcContext, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rpcContext = metadata.AppendToOutgoingContext(
			rpcContext, membershipControlTokenHeader, token,
		)

		response, err = kvpb.NewKeyValueStoreClient(connection).
			GetJoinResumeDecision(rpcContext, request)
	}
	if err != nil {
		return err
	}

	oldID := previous.Identity()
	candidateID := candidate.Identity()
	if response == nil ||
		response.GetCoordinatorNodeId() != coordinator.ID ||
		response.GetActiveEpoch() != oldID.Epoch ||
		!bytes.Equal(response.GetActiveDigest(), oldID.Digest[:]) ||
		response.GetCandidateEpoch() != candidateID.Epoch ||
		!bytes.Equal(response.GetCandidateDigest(), candidateID.Digest[:]) {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator returned a different resume identity",
		)
	}
	if requireClientRelease && !response.GetClientsReleaseDecided() {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator has not authorized client release",
		)
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
func (s *GRPCServer) requireJoinResumeDecision(
	ctx context.Context,
	previous, candidate membership.Configuration,
	request *kvpb.PrepareJoinPauseRequest,
) error {
	return s.requireJoinResumeAuthorization(
		ctx, previous, candidate, request, false,
	)
}

func (s *GRPCServer) requireJoinClientReleaseDecision(
	ctx context.Context,
	previous, candidate membership.Configuration,
	request *kvpb.PrepareJoinPauseRequest,
) error {
	return s.requireJoinResumeAuthorization(
		ctx, previous, candidate, request, true,
	)
}
