package main

import (
	"context"
	"flag"
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
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/server"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func main() {
	nodeID := flag.String("node-id", "node-1", "Unique node identifier")
	address := flag.String("address", ":50051", "Address to listen on")
	dataDir := flag.String("data-dir", "data", "Directory for persistent data")
	nodes := flag.String(
		"nodes",
		"node-1=localhost:50051,node-2=localhost:50052",
		"Cluster nodes: node-id=address,node-id=address",
	)
	replicationFactor := flag.Int(
		"replication-factor",
		2,
		"Number of physical copies stored for each key",
	)
	readQuorum := flag.Int(
		"read-quorum",
		1,
		"Number of successful replica reads required",
	)

	writeQuorum := flag.Int(
		"write-quorum",
		2,
		"Number of successful replica writes required",
	)
	antiEntropyInterval := flag.Duration(
		"anti-entropy-interval",
		10*time.Second,
		"Interval between anti-entropy synchronization rounds",
	)
	bootstrapMembership := flag.Bool(
		"bootstrap-membership",
		false,
		"Create this node's active membership file on first setup",
	)
	membershipTokenFile := flag.String(
		"membership-token-file",
		"",
		"File containing a shared 64-character hex token for local membership control",
	)
	flag.Parse()

	clusterRing, err := buildRing(*nodes)
	if err != nil {
		log.Fatalf("failed to build ring: %v", err)
	}
	if *replicationFactor <= 0 {
		log.Fatalf(
			"replication factor must be greater than zero",
		)
	}

	if *replicationFactor > clusterRing.NodeCount() {
		log.Fatalf(
			"replication factor %d exceeds physical node count %d",
			*replicationFactor,
			clusterRing.NodeCount(),
		)
	}
	if *readQuorum <= 0 || *readQuorum > *replicationFactor {
		log.Fatalf(
			"read quorum must be between 1 and replication factor %d",
			*replicationFactor,
		)
	}

	if *writeQuorum <= 0 || *writeQuorum > *replicationFactor {
		log.Fatalf(
			"write quorum must be between 1 and replication factor %d",
			*replicationFactor,
		)
	}

	if *readQuorum+*writeQuorum <= *replicationFactor {
		log.Printf(
			"warning: R + W <= N; read and write quorums may not overlap",
		)
	}

	walPath := filepath.Join(*dataDir, *nodeID, "wal.log")

	writeAheadLog, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer writeAheadLog.Close()

	activePath := filepath.Join(*dataDir, *nodeID, "membership-active.json")
	active, err := membership.LoadOrBootstrapActive(
		activePath,
		clusterRing,
		*replicationFactor,
		*bootstrapMembership,
	)
	if err != nil {
		log.Fatalf("active membership: %v", err)
	}

	localMember := false
	for _, node := range active.Nodes() {
		if node.ID == *nodeID {
			localMember = true
			break
		}
	}
	if !localMember {
		log.Fatalf("node %s is absent from active membership", *nodeID)
	}

	log.Printf("active membership epoch=%d; file=%s",
		active.Identity().Epoch, activePath)

	kvStore, err := store.NewMap(writeAheadLog)
	if err != nil {
		log.Fatalf("failed to recover store: %v", err)
	}

	hintPath := filepath.Join(*dataDir, *nodeID, "hints.log")

	hintQueue, err := handoff.OpenQueue(hintPath)
	if err != nil {
		log.Fatalf("failed to open hint queue: %v", err)
	}

	defer func() {
		if err := hintQueue.Close(); err != nil {
			log.Printf("failed to close hint queue: %v", err)
		}
	}()

	log.Printf(
		"node %s recovered %d pending hints; journal: %s",
		*nodeID,
		len(hintQueue.Pending()),
		hintPath,
	)

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	kvService := server.New(
		kvStore,
		clusterRing,
		*nodeID,
		*replicationFactor,
		*readQuorum,
		*writeQuorum,
		hintQueue,
	)
	if err := kvService.SetActiveMembership(active); err != nil {
		log.Fatalf("configure server membership: %v", err)
	}
	pausePath := filepath.Join(*dataDir, *nodeID, "membership-pause.json")
	if err := kvService.ConfigureJoinPause(pausePath); err != nil {
		log.Fatalf("restore join pause: %v", err)
	}

	if *membershipTokenFile != "" {
		info, err := os.Stat(*membershipTokenFile)
		if err != nil {
			log.Fatalf("membership token file: %v", err)
		}
		if info.Mode().Perm()&0077 != 0 {
			log.Fatal("membership token file must not be accessible by group or others")
		}

		data, err := os.ReadFile(*membershipTokenFile)
		if err != nil {
			log.Fatalf("read membership token: %v", err)
		}
		if err := kvService.ConfigureMembershipControlToken(
			strings.TrimSpace(string(data)),
		); err != nil {
			log.Fatalf("configure membership control: %v", err)
		}
	}

	kvpb.RegisterKeyValueStoreServer(
		grpcServer,
		kvService,
	)

	log.Printf(
		"node %s listening on %s; WAL: %s; N=%d R=%d W=%d; anti-entropy=%s",
		*nodeID,
		*address,
		walPath,
		*replicationFactor,
		*readQuorum,
		*writeQuorum,
		*antiEntropyInterval,
	)

	serverContext, cancelServer := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancelServer()

	if err := kvService.StartAntiEntropy(
		serverContext,
		*antiEntropyInterval,
	); err != nil {
		log.Fatalf(
			"failed to start anti-entropy worker: %v",
			err,
		)
	}

	hintWorkerDone := make(chan struct{})

	go func() {
		defer close(hintWorkerDone)

		if err := kvService.RunHintDelivery(
			serverContext,
			3*time.Second,
		); err != nil {
			log.Printf("hint worker stopped after an error: %v", err)

			// Stop the server rather than silently running without delivery.
			cancelServer()
		}
	}()

	serveResult := make(chan error, 1)

	go func() {
		serveResult <- grpcServer.Serve(listener)
	}()

	select {
	case <-serverContext.Done():
		log.Printf("node %s shutting down", *nodeID)

	case err := <-serveResult:
		if err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}

	cancelServer()

	// Stop anti-entropy before shutting down gRPC so it cannot start
	// another peer request during server shutdown.
	kvService.StopAntiEntropy()

	// Allow active requests to finish, with a bounded shutdown wait.

	// Allow active requests to finish, with a bounded shutdown wait.
	grpcStopped := make(chan struct{})

	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()

	shutdownTimer := time.NewTimer(5 * time.Second)

	select {
	case <-grpcStopped:
		if !shutdownTimer.Stop() {
			// No further use of the timer is needed.
		}

	case <-shutdownTimer.C:
		log.Printf("forcing gRPC shutdown")
		grpcServer.Stop()
		<-grpcStopped
	}

	// The journal remains open until the worker has exited.
	<-hintWorkerDone

	log.Printf("node %s stopped", *nodeID)
}

func buildRing(nodesText string) (*ring.Ring, error) {
	clusterRing := ring.New(100)

	for _, rawNode := range strings.Split(nodesText, ",") {
		parts := strings.SplitN(strings.TrimSpace(rawNode), "=", 2)

		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid node: %q", rawNode)
		}

		clusterRing.AddNode(ring.Node{
			ID:      parts[0],
			Address: parts[1],
		})
	}

	return clusterRing, nil
}
