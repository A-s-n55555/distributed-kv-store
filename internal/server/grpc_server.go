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
	"time"
)

type GRPCServer struct {
	kvpb.UnimplementedKeyValueStoreServer

	store             *store.Map
	ring              *ring.Ring
	nodeID            string
	replicationFactor int
	readQuorum        int
	writeQuorum       int
}

func New(
	kvStore *store.Map,
	clusterRing *ring.Ring,
	nodeID string,
	replicationFactor int,
	readQuorum int,
	writeQuorum int,
) *GRPCServer {
	return &GRPCServer{
		store:             kvStore,
		ring:              clusterRing,
		nodeID:            nodeID,
		replicationFactor: replicationFactor,
		readQuorum:        readQuorum,
		writeQuorum:       writeQuorum,
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
	replicaNodes, err := s.replicaNodesFor(request.GetKey())
	if err != nil {
		return nil, err
	}

	successfulWrites := 0
	var lastError error

	for _, replicaNode := range replicaNodes {
		if replicaNode.ID == s.nodeID {
			err = s.putLocal(request)
		} else {
			err = s.forwardReplicaPut(
				ctx,
				replicaNode,
				request,
			)
		}

		if err != nil {
			lastError = err
			continue
		}

		successfulWrites++
	}

	if successfulWrites < s.writeQuorum {
		return nil, status.Errorf(
			codes.Unavailable,
			"write quorum not reached: successful=%d required=%d: %v",
			successfulWrites,
			s.writeQuorum,
			lastError,
		)
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
	replicaNodes, err := s.replicaNodesFor(request.GetKey())
	if err != nil {
		return nil, err
	}

	readResults := make(
		[]*kvpb.GetResponse,
		0,
		s.readQuorum,
	)

	var lastError error

	for _, replicaNode := range replicaNodes {
		var response *kvpb.GetResponse

		if replicaNode.ID == s.nodeID {
			response = s.getLocal(request)
		} else {
			response, err = s.forwardReplicaGet(
				ctx,
				replicaNode,
				request,
			)

			if err != nil {
				lastError = err
				continue
			}
		}

		readResults = append(readResults, response)

		if len(readResults) == s.readQuorum {
			break
		}
	}

	if len(readResults) < s.readQuorum {
		return nil, status.Errorf(
			codes.Unavailable,
			"read quorum not reached: successful=%d required=%d: %v",
			len(readResults),
			s.readQuorum,
			lastError,
		)
	}

	firstResult := readResults[0]

	for _, result := range readResults[1:] {
		if !sameReadResult(firstResult, result) {
			return nil, status.Error(
				codes.Aborted,
				"replica values disagree; conflict resolution is not implemented",
			)
		}
	}

	return firstResult, nil
}

func sameReadResult(
	first *kvpb.GetResponse,
	second *kvpb.GetResponse,
) bool {
	if first.GetFound() != second.GetFound() {
		return false
	}

	// Both replicas agree that the key does not exist.
	if !first.GetFound() {
		return true
	}

	return first.GetValue() == second.GetValue()
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
	replicaNodes, err := s.replicaNodesFor(request.GetKey())
	if err != nil {
		return nil, err
	}

	successfulDeletes := 0
	var lastError error

	for _, replicaNode := range replicaNodes {
		if replicaNode.ID == s.nodeID {
			err = s.deleteLocal(request)
		} else {
			err = s.forwardReplicaDelete(
				ctx,
				replicaNode,
				request,
			)
		}

		if err != nil {
			lastError = err
			continue
		}

		successfulDeletes++
	}

	if successfulDeletes < s.writeQuorum {
		return nil, status.Errorf(
			codes.Unavailable,
			"delete quorum not reached: successful=%d required=%d: %v",
			successfulDeletes,
			s.writeQuorum,
			lastError,
		)
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
