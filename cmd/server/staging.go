package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/handoff"
	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/server"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func loadStagingConfiguration(
	nodeID, address, activeFile, candidateFile string,
) (membership.Configuration, error) {
	if activeFile == "" || candidateFile == "" {
		return membership.Configuration{}, fmt.Errorf(
			"staging requires active and candidate files",
		)
	}

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return membership.Configuration{}, fmt.Errorf("staging address: %w", err)
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") &&
		(ip == nil || !ip.IsLoopback()) {
		return membership.Configuration{}, fmt.Errorf(
			"staging address must be loopback",
		)
	}

	active, err := membership.LoadCandidate(activeFile)
	if err != nil {
		return membership.Configuration{}, fmt.Errorf("load old membership: %w", err)
	}
	candidate, joining, err := membership.LoadJoinCandidate(candidateFile, active)
	if err != nil {
		return membership.Configuration{}, fmt.Errorf("load join candidate: %w", err)
	}
	if joining.ID != nodeID || joining.Address != address {
		return membership.Configuration{}, fmt.Errorf(
			"staging node ID and address must match the joining node",
		)
	}
	return candidate, nil
}

func runStagingJoin(
	nodeID, address, dataDir, activeFile, candidateFile string,
	tokenFile string,
	readQuorum, writeQuorum int,
	antiEntropyInterval time.Duration,
) error {
	if antiEntropyInterval <= 0 {
		return fmt.Errorf("anti-entropy interval must be positive")
	}
	pausePath := filepath.Join(dataDir, nodeID, "membership-pause.json")

	current, candidate, err := loadStagingJoinState(
		nodeID, address, activeFile, candidateFile, pausePath,
	)
	if err != nil {
		return err
	}

	walPath := filepath.Join(dataDir, nodeID, "wal.log")
	writeAheadLog, err := wal.Open(walPath)
	if err != nil {
		return fmt.Errorf("open staging WAL: %w", err)
	}
	defer writeAheadLog.Close()

	kvStore, err := store.NewMap(writeAheadLog)
	if err != nil {
		return fmt.Errorf("recover staged records: %w", err)
	}

	replicationFactor := candidate.ReplicationFactor()
	if readQuorum < 1 || readQuorum > replicationFactor ||
		writeQuorum < 1 || writeQuorum > replicationFactor {
		return fmt.Errorf(
			"read and write quorums must be between 1 and %d",
			replicationFactor,
		)
	}

	hintQueue, err := handoff.OpenQueue(
		filepath.Join(dataDir, nodeID, "hints.log"),
	)
	if err != nil {
		return fmt.Errorf("open staging hint queue: %w", err)
	}
	defer hintQueue.Close()

	target := server.New(
		kvStore,
		candidate.Ring(),
		nodeID,
		replicationFactor,
		readQuorum,
		writeQuorum,
		hintQueue,
	)

	if tokenFile != "" {
		info, err := os.Stat(tokenFile)
		if err != nil {
			return fmt.Errorf("membership token file: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return fmt.Errorf(
				"membership token must be a regular file accessible only by its owner",
			)
		}

		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return fmt.Errorf("read membership token: %w", err)
		}
		if err := target.ConfigureMembershipControlToken(
			strings.TrimSpace(string(data)),
		); err != nil {
			return fmt.Errorf("configure staging membership control: %w", err)
		}
	}

	stagingService, err := server.NewStagingService(target)
	if err != nil {
		return err
	}

	if err := stagingService.ConfigureJoin(current, candidate); err != nil {
		return fmt.Errorf("configure staging join: %w", err)
	}
	if err := stagingService.ConfigurePromotion(pausePath); err != nil {
		return fmt.Errorf("configure promotion recovery: %w", err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for staging: %w", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	kvpb.RegisterKeyValueStoreServer(grpcServer, stagingService)

	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	workersDone := make(chan struct{})

	go func() {
		defer close(workersDone)

		err := stagingService.RunPromotedWorkers(ctx, antiEntropyInterval)
		if ctx.Err() == nil {
			log.Printf("promoted-node workers stopped: %v", err)
			stop()
		}
	}()

	// Registered after the WAL/queue close defers, so workers exit first.
	defer func() {
		stop()
		<-workersDone
	}()

	serveResult := make(chan error, 1)
	go func() { serveResult <- grpcServer.Serve(listener) }()

	log.Printf(
		"node %s staging candidate epoch=%d on %s; WAL: %s",
		nodeID, candidate.Identity().Epoch, address, walPath,
	)

	select {
	case err := <-serveResult:
		return err
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
		<-stopped
	}
	return nil
}
