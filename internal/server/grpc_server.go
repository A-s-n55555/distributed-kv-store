package server

import (
	"context"
	"strconv"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	kvpb.UnimplementedKeyValueStoreServer

	store  *store.Map
	ring   *ring.Ring
	nodeID string
}

func New(
	kvStore *store.Map,
	clusterRing *ring.Ring,
	nodeID string,
) *GRPCServer {
	return &GRPCServer{
		store:  kvStore,
		ring:   clusterRing,
		nodeID: nodeID,
	}
}

func (s *GRPCServer) nodeFor(key int64) (ring.Node, error) {
	node, found := s.ring.GetNode(strconv.FormatInt(key, 10))
	if !found {
		return ring.Node{}, status.Error(
			codes.Unavailable,
			"cluster has no nodes",
		)
	}

	return node, nil
}

func (s *GRPCServer) Put(
	ctx context.Context,
	request *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	node, err := s.nodeFor(request.GetKey())
	if err != nil {
		return nil, err
	}

	if node.ID != s.nodeID {
		return s.forwardPut(ctx, node.Address, request)
	}

	if err := s.store.Put(request.GetKey(), request.GetValue()); err != nil {
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}

	return &kvpb.PutResponse{}, nil
}

func (s *GRPCServer) Get(
	ctx context.Context,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	node, err := s.nodeFor(request.GetKey())
	if err != nil {
		return nil, err
	}

	if node.ID != s.nodeID {
		return s.forwardGet(ctx, node.Address, request)
	}

	value, found := s.store.Get(request.GetKey())

	return &kvpb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (s *GRPCServer) Delete(
	ctx context.Context,
	request *kvpb.DeleteRequest,
) (*kvpb.DeleteResponse, error) {
	node, err := s.nodeFor(request.GetKey())
	if err != nil {
		return nil, err
	}

	if node.ID != s.nodeID {
		return s.forwardDelete(ctx, node.Address, request)
	}

	if err := s.store.Delete(request.GetKey()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}

	return &kvpb.DeleteResponse{}, nil
}

func (s *GRPCServer) forwardPut(
	ctx context.Context,
	address string,
	request *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect failed: %v", err)
	}
	defer connection.Close()

	return kvpb.NewKeyValueStoreClient(connection).Put(ctx, request)
}

func (s *GRPCServer) forwardGet(
	ctx context.Context,
	address string,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect failed: %v", err)
	}
	defer connection.Close()

	return kvpb.NewKeyValueStoreClient(connection).Get(ctx, request)
}

func (s *GRPCServer) forwardDelete(
	ctx context.Context,
	address string,
	request *kvpb.DeleteRequest,
) (*kvpb.DeleteResponse, error) {
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect failed: %v", err)
	}
	defer connection.Close()

	return kvpb.NewKeyValueStoreClient(connection).Delete(ctx, request)
}
