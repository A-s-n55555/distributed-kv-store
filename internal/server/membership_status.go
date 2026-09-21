package server

import (
	"context"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetMembershipStatus reports the active membership and local join pause.
func (s *GRPCServer) GetMembershipStatus(
	ctx context.Context,
	request *kvpb.MembershipStatusRequest,
) (*kvpb.MembershipStatusResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	// Use the same lock order as the write gate.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	active := s.activeMembership.Identity()
	if active.Epoch == 0 {
		return nil, status.Error(codes.FailedPrecondition, "active membership is not configured")
	}
	if s.writesPaused != s.replicasPaused {
		return nil, status.Error(codes.FailedPrecondition, "local write gates disagree")
	}

	response := &kvpb.MembershipStatusResponse{
		NodeId:       s.nodeID,
		ActiveEpoch:  active.Epoch,
		ActiveDigest: append([]byte(nil), active.Digest[:]...),
		WritesPaused: s.writesPaused,
	}
	if s.pendingJoin != nil {
		response.PendingEpoch = s.pendingJoin.Epoch
		response.PendingDigest = append([]byte(nil), s.pendingJoin.Digest[:]...)
	}
	return response, nil
}
