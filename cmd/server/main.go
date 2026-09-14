package main

import (
	"log"
	"net"

	"github.com/A-s-n55555/distributed-kv-store/internal/server"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func main() {
	writeAheadLog, err := wal.Open("data/wal.log")
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer writeAheadLog.Close()

	kvStore, err := store.NewMap(writeAheadLog)
	if err != nil {
		log.Fatalf("failed to recover store: %v", err)
	}

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	kvpb.RegisterKeyValueStoreServer(
		grpcServer,
		server.New(kvStore),
	)

	log.Println("gRPC server listening on port 50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
