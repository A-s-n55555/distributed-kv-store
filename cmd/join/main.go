package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
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

func loopbackAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("membership control requires loopback address: %s", address)
	}
	return nil
}

func readToken(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("token must be an owner-only regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(decoded) != 32 {
		return "", fmt.Errorf("token must contain 64 hexadecimal characters")
	}
	return hex.EncodeToString(decoded), nil
}

func connect(address string) (kvpb.KeyValueStoreClient, func(), error) {
	if err := loopbackAddress(address); err != nil {
		return nil, nil, err
	}
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return kvpb.NewKeyValueStoreClient(connection),
		func() { _ = connection.Close() }, nil
}

func matches(
	response *kvpb.MembershipStatusResponse,
	identity membership.Identity,
) bool {
	return response != nil &&
		response.GetActiveEpoch() == identity.Epoch &&
		bytes.Equal(response.GetActiveDigest(), identity.Digest[:])
}

func run() error {
	previousPath := flag.String(
		"previous-file", "", "Immutable old membership JSON",
	)
	candidatePath := flag.String(
		"candidate-file", "", "Validated candidate membership JSON",
	)
	tokenPath := flag.String(
		"membership-token-file", "", "Shared membership control token file",
	)
	timeout := flag.Duration(
		"timeout", 5*time.Minute, "Overall join attempt timeout",
	)
	flag.Parse()

	if *previousPath == "" || *candidatePath == "" || *tokenPath == "" {
		return fmt.Errorf(
			"-previous-file, -candidate-file and -membership-token-file are required",
		)
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	previous, err := membership.LoadCandidate(*previousPath)
	if err != nil {
		return fmt.Errorf("load previous membership: %w", err)
	}
	candidate, joining, err := membership.LoadJoinCandidate(
		*candidatePath, previous,
	)
	if err != nil {
		return fmt.Errorf("load join candidate: %w", err)
	}
	coordinator, err := membership.JoinCoordinator(previous)
	if err != nil {
		return err
	}

	token, err := readToken(*tokenPath)
	if err != nil {
		return fmt.Errorf("read membership token: %w", err)
	}
	coordinatorClient, closeCoordinator, err := connect(coordinator.Address)
	if err != nil {
		return fmt.Errorf("connect coordinator: %w", err)
	}
	defer closeCoordinator()

	joiningClient, closeJoining, err := connect(joining.Address)
	if err != nil {
		return fmt.Errorf("connect joining node: %w", err)
	}
	defer closeJoining()

	oldID, candidateID := previous.Identity(), candidate.Identity()
	request := &kvpb.PrepareJoinPauseRequest{
		ActiveEpoch:        oldID.Epoch,
		ActiveDigest:       append([]byte(nil), oldID.Digest[:]...),
		CandidateEpoch:     candidateID.Epoch,
		CandidateDigest:    append([]byte(nil), candidateID.Digest[:]...),
		JoiningNodeId:      joining.ID,
		JoiningNodeAddress: joining.Address,
	}

	base, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx := metadata.AppendToOutgoingContext(
		base, "x-kv-membership-control-token", token,
	)

	readStatus := func(
		client kvpb.KeyValueStoreClient,
	) (*kvpb.MembershipStatusResponse, error) {
		return client.GetMembershipStatus(
			ctx, &kvpb.MembershipStatusRequest{},
		)
	}

	coordinatorStatus, err := readStatus(coordinatorClient)
	if err != nil {
		return fmt.Errorf("read coordinator status: %w", err)
	}

	switch {
	case matches(coordinatorStatus, oldID):
		fmt.Println("Finishing catch-up and commit...")
		if _, err := coordinatorClient.CoordinateJoinCommit(
			ctx, request,
		); err != nil {
			return fmt.Errorf("commit phase: %w", err)
		}
		fallthrough

	case matches(coordinatorStatus, candidateID) &&
		!coordinatorStatus.GetReplicasReady() &&
		!coordinatorStatus.GetClientsReleased():
		fmt.Println("Activating old members...")
		if _, err := coordinatorClient.CoordinateJoinActivation(
			ctx, request,
		); err != nil {
			return fmt.Errorf("activation phase: %w", err)
		}

	case matches(coordinatorStatus, candidateID):
		fmt.Println("Activation already completed; continuing resume...")

	default:
		return fmt.Errorf("coordinator has an unrelated active membership")
	}

	coordinatorStatus, err = readStatus(coordinatorClient)
	if err != nil {
		return fmt.Errorf("read activated coordinator status: %w", err)
	}
	if !matches(coordinatorStatus, candidateID) {
		return fmt.Errorf("coordinator did not activate the candidate")
	}

	joiningStatus, err := readStatus(joiningClient)
	if status.Code(err) == codes.Unimplemented {
		fmt.Println("Promoting joining node...")
		if _, err := joiningClient.PromoteJoin(ctx, request); err != nil {
			return fmt.Errorf("promotion phase: %w", err)
		}
		joiningStatus, err = readStatus(joiningClient)
	}
	if err != nil {
		return fmt.Errorf("read joining-node status: %w", err)
	}
	if joiningStatus.GetNodeId() != joining.ID ||
		!matches(joiningStatus, candidateID) ||
		!joiningStatus.GetJoinCommitted() {
		return fmt.Errorf("joining node did not confirm promotion")
	}

	fmt.Println("Completing coordinated resume...")
	completed, err := coordinatorClient.CoordinateJoinResume(ctx, request)
	if err != nil {
		return fmt.Errorf("resume phase: %w", err)
	}
	if !matches(completed, candidateID) ||
		!completed.GetReplicasReady() ||
		!completed.GetClientsReleased() ||
		completed.GetWritesPaused() {
		return fmt.Errorf("coordinator did not confirm completed resume")
	}

	fmt.Printf(
		"Join complete: %s entered membership epoch %d\n",
		joining.ID, candidateID.Epoch,
	)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "join:", err)
		os.Exit(1)
	}
}
