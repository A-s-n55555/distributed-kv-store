package server

import (
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

func confirmsJoinReplicaReady(
	response *kvpb.MembershipStatusResponse,
	nodeID string,
	candidate membership.Identity,
) bool {
	return confirmsJoinCommit(response, nodeID, candidate, candidate) &&
		response.GetReplicasReady()
}

// enableAllJoinReplicas authorizes resume, enables replica updates on every
// candidate member, and verifies completion. Client gates remain closed.
func (s *GRPCServer) enableAllJoinReplicas(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := s.prepareJoinResume(ctx, candidate); err != nil {
		return err
	}

	s.writeMu.Lock()
	active := s.activeMembership
	path := s.joinPausePath
	token := s.membershipControlToken
	s.writeMu.Unlock()

	if active.Identity() != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"local membership changed",
		)
	}
	previous, err := membership.LoadActivatedJoin(path, active)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if err := s.requireJoinCoordinator(previous); err != nil {
		return err
	}
	if token == "" {
		return status.Error(
			codes.FailedPrecondition,
			"membership control token is not configured",
		)
	}

	joining, err := membership.ValidateJoinCandidate(previous, candidate)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	oldID, candidateID := previous.Identity(), candidate.Identity()
	request := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        oldID.Epoch,
		ActiveDigest:       append([]byte(nil), oldID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
		JoiningNodeId:      joining.ID,
		JoiningNodeAddress: joining.Address,
	}

	for _, node := range candidate.Nodes() {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}

		// Use authenticated gRPC for every member, including the coordinator.
		// No server locks are held while the RPC executes.
		host, _, err := net.SplitHostPort(node.Address)
		if err != nil {
			return status.Errorf(
				codes.FailedPrecondition,
				"invalid address for %s: %v", node.ID, err,
			)
		}
		ip := net.ParseIP(host)
		if !strings.EqualFold(host, "localhost") &&
			(ip == nil || !ip.IsLoopback()) {
			return status.Error(
				codes.FailedPrecondition,
				"membership control requires loopback addresses",
			)
		}

		response, rpcErr := func() (*kvpb.MembershipStatusResponse, error) {
			connection, err := grpc.NewClient(
				node.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				return nil, err
			}
			defer connection.Close()

			rpcContext, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			rpcContext = metadata.AppendToOutgoingContext(
				rpcContext, membershipControlTokenHeader, token,
			)

			return kvpb.NewKeyValueStoreClient(connection).
				EnableJoinReplicas(rpcContext, request)
		}()

		if rpcErr != nil {
			// The response may have been lost after readiness was persisted.
			var readErr error
			if node.ID == s.nodeID {
				response, readErr = s.GetMembershipStatus(
					ctx, &kvpb.MembershipStatusRequest{},
				)
			} else {
				response, readErr = s.fetchMembershipStatus(ctx, node)
			}
			if readErr != nil ||
				!confirmsJoinReplicaReady(response, node.ID, candidateID) {
				return status.Errorf(
					codes.Unavailable,
					"replica readiness of %s is uncertain: RPC: %v; status: %v",
					node.ID, rpcErr, readErr,
				)
			}
		}

		if !confirmsJoinReplicaReady(response, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s did not confirm replica readiness", node.ID,
			)
		}
	}

	// Recheck all members before declaring the phase complete.
	for _, node := range candidate.Nodes() {
		var response *kvpb.MembershipStatusResponse
		if node.ID == s.nodeID {
			response, err = s.GetMembershipStatus(
				ctx, &kvpb.MembershipStatusRequest{},
			)
		} else {
			response, err = s.fetchMembershipStatus(ctx, node)
		}
		if err != nil {
			return status.Errorf(
				codes.Unavailable,
				"verify replica readiness on %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinReplicaReady(response, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s is not replica-ready", node.ID,
			)
		}
	}

	return nil
}
