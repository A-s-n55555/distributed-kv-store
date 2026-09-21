package server

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestMembershipControlRequiresTokenAndLoopback(t *testing.T) {
	s := newRecordTestServer(t)
	token := strings.Repeat("ab", 32)

	loopback := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50051},
	})
	withToken := metadata.NewIncomingContext(
		loopback,
		metadata.Pairs(membershipControlTokenHeader, token),
	)

	if got := status.Code(s.authorizeMembershipControl(withToken)); got != codes.Unimplemented {
		t.Fatalf("disabled control: got %v, want Unimplemented", got)
	}
	if err := s.ConfigureMembershipControlToken(token); err != nil {
		t.Fatal(err)
	}
	if err := s.authorizeMembershipControl(withToken); err != nil {
		t.Fatalf("valid local control: %v", err)
	}

	missingToken := status.Code(s.authorizeMembershipControl(loopback))
	if missingToken != codes.PermissionDenied {
		t.Fatalf("missing token: got %v, want PermissionDenied", missingToken)
	}

	remote := peer.NewContext(withToken, &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.5"), Port: 50051},
	})
	if got := status.Code(s.authorizeMembershipControl(remote)); got != codes.PermissionDenied {
		t.Fatalf("non-loopback peer: got %v, want PermissionDenied", got)
	}
}
