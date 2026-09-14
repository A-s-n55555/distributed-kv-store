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
	flag.Parse()

	clusterRing, err := buildRing(*nodes)
	if err != nil {
		log.Fatalf("failed to build ring: %v", err)
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
		server.New(kvStore, clusterRing, *nodeID),
	)

	log.Printf(
		"node %s listening on %s; WAL: %s",
		*nodeID,
		*address,
		walPath,
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
