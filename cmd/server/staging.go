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
) error {
	candidate, err := loadStagingConfiguration(
		nodeID, address, activeFile, candidateFile,
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

	target := server.New(
		kvStore, nil, nodeID,
		candidate.ReplicationFactor(), 0, 0, nil,
	)
	stagingService, err := server.NewStagingService(target)
	if err != nil {
		return err
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
