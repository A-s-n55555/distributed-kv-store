package server

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const membershipControlTokenHeader = "x-kv-membership-control-token"

// ConfigureMembershipControlToken enables local membership control.
// The token must be 32 bytes encoded as hexadecimal text.
func (s *GRPCServer) ConfigureMembershipControlToken(hexToken string) error {
	decoded, err := hex.DecodeString(hexToken)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("membership control token must be 64 hexadecimal characters")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.membershipControlToken != "" {
		return fmt.Errorf("membership control token is already configured")
	}
	s.membershipControlToken = hex.EncodeToString(decoded)
	return nil
}

// authorizeMembershipControl must be called before a control RPC changes state.
func (s *GRPCServer) authorizeMembershipControl(ctx context.Context) error {
	s.writeMu.Lock()
	token := s.membershipControlToken
	s.writeMu.Unlock()

	if token == "" {
		return status.Error(codes.Unimplemented, "membership control is disabled")
	}

	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
		return status.Error(codes.PermissionDenied, "membership control requires a loopback peer")
	}
	address, ok := remote.Addr.(*net.TCPAddr)
	if !ok || !address.IP.IsLoopback() {
		return status.Error(codes.PermissionDenied, "membership control requires a loopback peer")
	}

	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.PermissionDenied, "membership control token is required")
	}
	values := incoming.Get(membershipControlTokenHeader)
	if len(values) != 1 ||
		subtle.ConstantTimeCompare([]byte(values[0]), []byte(token)) != 1 {
		return status.Error(codes.PermissionDenied, "invalid membership control token")
	}

	return nil
}
