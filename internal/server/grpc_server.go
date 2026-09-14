package server

// package server

import (
	"context"

	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	kvpb.UnimplementedKeyValueStoreServer
	store *store.Map
}

func New(kvStore *store.Map) *GRPCServer {
	return &GRPCServer{
		store: kvStore,
	}
}

func (s *GRPCServer) Put(
	_ context.Context,
	request *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	err := s.store.Put(request.GetKey(), request.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}

	return &kvpb.PutResponse{}, nil
}

func (s *GRPCServer) Get(
	_ context.Context,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	value, found := s.store.Get(request.GetKey())

	return &kvpb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (s *GRPCServer) Delete(
	_ context.Context,
	request *kvpb.DeleteRequest,
) (*kvpb.DeleteResponse, error) {
	err := s.store.Delete(request.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}

	return &kvpb.DeleteResponse{}, nil
}
