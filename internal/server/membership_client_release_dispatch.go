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

func confirmsJoinClientsReleased(
	response *kvpb.MembershipStatusResponse,
	nodeID string,
	candidate membership.Identity,
) bool {
	return response != nil &&
		response.GetNodeId() == nodeID &&
		response.GetActiveEpoch() == candidate.Epoch &&
		bytes.Equal(response.GetActiveDigest(), candidate.Digest[:]) &&
		response.GetPendingEpoch() == candidate.Epoch &&
		bytes.Equal(response.GetPendingDigest(), candidate.Digest[:]) &&
		response.GetJoinCommitted() &&
		response.GetReplicasReady() &&
		response.GetClientsReleased() &&
		!response.GetWritesPaused()
}

// releaseAllJoinClients completes client release without re-pausing members.
func (s *GRPCServer) releaseAllJoinClients(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := s.prepareJoinClientRelease(ctx, candidate); err != nil {
		return err
	}

	s.writeMu.Lock()
	active := s.activeMembership
	path := s.joinPausePath
	token := s.membershipControlToken
	s.writeMu.Unlock()

	if active.Identity() != candidate.Identity() {
		return status.Error(codes.FailedPrecondition, "local membership changed")
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

	readStatus := func(
		node ring.Node,
	) (*kvpb.MembershipStatusResponse, error) {
		if node.ID == s.nodeID {
			return s.GetMembershipStatus(ctx, &kvpb.MembershipStatusRequest{})
		}
		return s.fetchMembershipStatus(ctx, node)
	}

	for _, node := range candidate.Nodes() {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}

		observed, err := readStatus(node)
		if err != nil {
			return status.Errorf(
				codes.Unavailable, "inspect %s before release: %v", node.ID, err,
			)
		}
		if confirmsJoinClientsReleased(observed, node.ID, candidateID) {
			continue
		}
		if !confirmsJoinReplicaReady(observed, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s is neither replica-ready nor released", node.ID,
			)
		}

		host, _, err := net.SplitHostPort(node.Address)
		if err != nil {
			return status.Errorf(
				codes.FailedPrecondition, "invalid address for %s: %v", node.ID, err,
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
				ReleaseJoinClients(rpcContext, request)
		}()

		if rpcErr != nil {
			// A lost reply does not prove that release failed.
			observed, readErr := readStatus(node)
			if readErr != nil ||
				!confirmsJoinClientsReleased(observed, node.ID, candidateID) {
				return status.Errorf(
					codes.Unavailable,
					"release of %s is uncertain: RPC: %v; status: %v",
					node.ID, rpcErr, readErr,
				)
			}
			continue
		}
		if !confirmsJoinClientsReleased(response, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s did not confirm client release", node.ID,
			)
		}
	}

	for _, node := range candidate.Nodes() {
		response, err := readStatus(node)
		if err != nil {
			return status.Errorf(
				codes.Unavailable, "confirm release on %s: %v", node.ID, err,
			)
		}
		if !confirmsJoinClientsReleased(response, node.ID, candidateID) {
			return status.Errorf(
				codes.FailedPrecondition,
				"node %s has not confirmed client release", node.ID,
			)
		}
	}

	return nil
}

// resumeActivatedJoin coordinates both resume phases.
// Retries after release authorization must not re-enable the earlier phase.
func (s *GRPCServer) resumeActivatedJoin(
	ctx context.Context,
	candidate membership.Configuration,
) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	s.writeMu.Lock()
	active := s.activeMembership
	path := s.joinPausePath
	s.writeMu.Unlock()

	if active.Identity() != candidate.Identity() {
		return status.Error(
			codes.FailedPrecondition,
			"requested candidate is not active",
		)
	}
	previous, err := membership.LoadActivatedJoin(path, active)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if err := s.requireJoinCoordinator(previous); err != nil {
		return err
	}

	releaseDecided, err := membership.LoadJoinClientReleaseDecision(path, active)
	if err != nil {
		return status.Errorf(
			codes.Internal, "inspect client-release decision: %v", err,
		)
	}

	if !releaseDecided {
		if err := s.enableAllJoinReplicas(ctx, candidate); err != nil {
			return err
		}
	}
	return s.releaseAllJoinClients(ctx, candidate)
}
