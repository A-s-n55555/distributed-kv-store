package server

import (
	"context"
	"strconv"
	"sync"

	"github.com/A-s-n55555/distributed-kv-store/internal/handoff"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"time"
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

func (s *GRPCServer) putLocal(
	request *kvpb.PutRequest,
) error {
	if err := s.store.Put(
		request.GetKey(),
		request.GetValue(),
	); err != nil {
		return status.Errorf(
			codes.Internal,
			"local put failed: %v",
			err,
		)
	}

	return nil
}

// ReplicaPut stores a replicated value directly on this node.
// It must not perform routing or further replication.
func (s *GRPCServer) ReplicaPut(
	_ context.Context,
	request *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	if err := s.putLocal(request); err != nil {
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

	record, exists, err := s.readVersionedQuorum(
		ctx,
		request.GetKey(),
	)
	if err != nil {
		return nil, err
	}

	if !exists || record.Deleted {
		return &kvpb.GetResponse{
			Found: false,
		}, nil
	}

	return &kvpb.GetResponse{
		Value: record.Value,
		Found: true,
	}, nil
}

func (s *GRPCServer) getLocal(
	request *kvpb.GetRequest,
) *kvpb.GetResponse {
	value, found := s.store.Get(request.GetKey())

	return &kvpb.GetResponse{
		Value: value,
		Found: found,
	}
}

// ReplicaGet reads directly from this node's local store.
// It must not route the request to another node.
func (s *GRPCServer) ReplicaGet(
	_ context.Context,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	return s.getLocal(request), nil
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
func (s *GRPCServer) deleteLocal(
	request *kvpb.DeleteRequest,
) error {
	if err := s.store.Delete(request.GetKey()); err != nil {
		return status.Errorf(
			codes.Internal,
			"local delete failed: %v",
			err,
		)
	}

	return nil
}

// ReplicaDelete removes a replicated value directly from this node.
// It must not perform routing or further replication.
func (s *GRPCServer) ReplicaDelete(
	_ context.Context,
	request *kvpb.DeleteRequest,
) (*kvpb.DeleteResponse, error) {
	if err := s.deleteLocal(request); err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{}, nil
}

func (s *GRPCServer) forwardReplicaPut(
	ctx context.Context,
	node ring.Node,
	request *kvpb.PutRequest,
) error {
	connection, err := grpc.NewClient(
		node.Address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return status.Errorf(
			codes.Unavailable,
			"connect to replica %s failed: %v",
			node.ID,
			err,
		)
	}
	defer connection.Close()

	replicaContext, cancel := context.WithTimeout(
		ctx,
		2*time.Second,
	)
	defer cancel()

	client := kvpb.NewKeyValueStoreClient(connection)

	if _, err := client.ReplicaPut(
		replicaContext,
		request,
	); err != nil {
		return status.Errorf(
			codes.Unavailable,
			"replication to %s failed: %v",
			node.ID,
			err,
		)
	}

	return nil
}

func (s *GRPCServer) forwardReplicaGet(
	ctx context.Context,
	node ring.Node,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	connection, err := grpc.NewClient(
		node.Address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Unavailable,
			"connect to replica %s failed: %v",
			node.ID,
			err,
		)
	}
	defer connection.Close()

	// Prevent an unavailable replica from blocking forever.
	replicaContext, cancel := context.WithTimeout(
		ctx,
		2*time.Second,
	)
	defer cancel()

	client := kvpb.NewKeyValueStoreClient(connection)

	response, err := client.ReplicaGet(
		replicaContext,
		request,
	)
	if err != nil {
		return nil, status.Errorf(
			codes.Unavailable,
			"read from replica %s failed: %v",
			node.ID,
			err,
		)
	}

	return response, nil
}

func (s *GRPCServer) forwardReplicaDelete(
	ctx context.Context,
	node ring.Node,
	request *kvpb.DeleteRequest,
) error {
	connection, err := grpc.NewClient(
		node.Address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return status.Errorf(
			codes.Unavailable,
			"connect to replica %s failed: %v",
			node.ID,
			err,
		)
	}
	defer connection.Close()

	replicaContext, cancel := context.WithTimeout(
		ctx,
		2*time.Second,
	)
	defer cancel()

	client := kvpb.NewKeyValueStoreClient(connection)

	if _, err := client.ReplicaDelete(
		replicaContext,
		request,
	); err != nil {
		return status.Errorf(
			codes.Unavailable,
			"replica deletion on %s failed: %v",
			node.ID,
			err,
		)
	}

	return nil
}
