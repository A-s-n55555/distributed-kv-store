package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"

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

	kvStore, err := store.NewMap(writeAheadLog)
	if err != nil {
		log.Fatalf("failed to recover store: %v", err)
	}

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	kvpb.RegisterKeyValueStoreServer(
		grpcServer,
		server.New(
			kvStore,
			clusterRing,
			*nodeID,
			*replicationFactor,
			*readQuorum,
			*writeQuorum,
		),
	)

	log.Printf(
		"node %s listening on %s; WAL: %s; N=%d R=%d W=%d",
		*nodeID,
		*address,
		walPath,
		*replicationFactor,
		*readQuorum,
		*writeQuorum,
	)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
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
