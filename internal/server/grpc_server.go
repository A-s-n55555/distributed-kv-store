package server

import (
	"context"
	"strconv"
	"sync"

	"github.com/A-s-n55555/distributed-kv-store/internal/handoff"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	kvpb.UnimplementedKeyValueStoreServer

	writeMu sync.Mutex

	store             *store.Map
	ring              *ring.Ring
	nodeID            string
	replicationFactor int
	readQuorum        int
	writeQuorum       int
	hintQueue         *handoff.Queue
}

func New(
	kvStore *store.Map,
	clusterRing *ring.Ring,
	nodeID string,
	replicationFactor int,
	readQuorum int,
	writeQuorum int,
	hintQueue *handoff.Queue,
) *GRPCServer {
	return &GRPCServer{
		store:             kvStore,
		ring:              clusterRing,
		nodeID:            nodeID,
		replicationFactor: replicationFactor,
		readQuorum:        readQuorum,
		writeQuorum:       writeQuorum,
		hintQueue:         hintQueue,
	}
}

func (s *GRPCServer) replicaNodesFor(
	key int64,
) ([]ring.Node, error) {
	replicaNodes := s.ring.GetReplicas(
		strconv.FormatInt(key, 10),
		s.replicationFactor,
	)

	if len(replicaNodes) != s.replicationFactor {
		return nil, status.Errorf(
			codes.Unavailable,
			"cluster has %d nodes; replication factor is %d",
			len(replicaNodes),
			s.replicationFactor,
		)
	}

	return replicaNodes, nil
}

func (s *GRPCServer) Put(
	ctx context.Context,
	request *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if err := s.coordinateVersionedWrite(
		ctx,
		request.GetKey(),
		request.GetValue(),
		false,
	); err != nil {
		return nil, err
	}

	return &kvpb.PutResponse{}, nil
}

func (s *GRPCServer) Get(
	ctx context.Context,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	versions, err := s.readSiblingQuorum(
		ctx,
		request.GetKey(),
	)
	if err != nil {
		return nil, err
	}

	return buildVersionedGetResponse(versions), nil
}
func (s *GRPCServer) Delete(
	ctx context.Context,
	request *kvpb.DeleteRequest,
) (*kvpb.DeleteResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if err := s.coordinateVersionedWrite(
		ctx,
		request.GetKey(),
		"",
		true,
	); err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{}, nil
}
