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

const membershipDecisionRPCTimeout = 2 * time.Second

// requireJoinDecision is called by authenticated membership RPC handlers.
// It checks the coordinator's durable decision before local mutation.
func (s *GRPCServer) requireJoinDecision(
	ctx context.Context,
	current, candidate membership.Configuration,
	want kvpb.JoinDecision,
) error {
	if want != kvpb.JoinDecision_JOIN_DECISION_COMMIT &&
		want != kvpb.JoinDecision_JOIN_DECISION_ABORT {
		return status.Error(codes.InvalidArgument, "commit or abort decision is required")
	}

	joining, err := membership.ValidateJoinCandidate(current, candidate)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid join candidate: %v", err)
	}
	coordinator, err := membership.JoinCoordinator(current)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}

	activeID, candidateID := current.Identity(), candidate.Identity()
	request := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        activeID.Epoch,
		ActiveDigest:       append([]byte(nil), activeID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
		JoiningNodeId:      joining.ID,
		JoiningNodeAddress: joining.Address,
	}

	s.writeMu.Lock()
	localActive := s.activeMembership.Identity()
	token := s.membershipControlToken
	s.writeMu.Unlock()

	if localActive != activeID {
		return status.Error(codes.FailedPrecondition, "local active membership changed")
	}

	var response *kvpb.JoinDecisionResponse
	if coordinator.ID == s.nodeID {
		// ctx retains the incoming RPC's authorization information.
		response, err = s.GetJoinDecision(ctx, request)
	} else {
		host, _, addressErr := net.SplitHostPort(coordinator.Address)
		if addressErr != nil {
			return status.Errorf(
				codes.FailedPrecondition,
				"invalid coordinator address: %v", addressErr,
			)
		}
		if !strings.EqualFold(host, "localhost") {
			ip := net.ParseIP(host)
			if ip == nil || !ip.IsLoopback() {
				return status.Error(
					codes.FailedPrecondition,
					"membership control requires a loopback address",
				)
			}
		}
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

		rpcContext, cancel := context.WithTimeout(ctx, membershipDecisionRPCTimeout)
		defer cancel()
		rpcContext = metadata.AppendToOutgoingContext(
			rpcContext, membershipControlTokenHeader, token,
		)

		response, err = kvpb.NewKeyValueStoreClient(connection).
			GetJoinDecision(rpcContext, request)
	}
	if err != nil {
		return err
	}

	if response == nil ||
		response.GetCoordinatorNodeId() != coordinator.ID ||
		response.GetActiveEpoch() != activeID.Epoch ||
		!bytes.Equal(response.GetActiveDigest(), activeID.Digest[:]) ||
		response.GetCandidateEpoch() != candidateID.Epoch ||
		!bytes.Equal(response.GetCandidateDigest(), candidateID.Digest[:]) {
		return status.Error(
			codes.FailedPrecondition,
			"coordinator returned a different join identity",
		)
	}
	if response.GetDecision() != want {
		return status.Errorf(
			codes.FailedPrecondition,
			"coordinator decision is %s; requested %s",
			response.GetDecision(), want,
		)
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
