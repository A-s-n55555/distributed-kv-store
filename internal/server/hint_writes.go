package server

import (
	"fmt"
	"log"

	"github.com/A-s-n55555/distributed-kv-store/internal/handoff"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) saveFailedReplicaHint(
	node ring.Node,
	key int64,
	record store.Record,
	writeError error,
) error {
	// Local storage errors are not remote delivery failures.
	if node.ID == s.nodeID {
		return nil
	}

	switch status.Code(writeError) {
	case codes.Unavailable, codes.DeadlineExceeded:
		// These failures are eligible for deferred delivery.
	default:
		return nil
	}

	if s.hintQueue == nil {
		return fmt.Errorf("hint queue is not configured")
	}

	hint, err := handoff.NewHint(node, key, record)
	if err != nil {
		return fmt.Errorf("create replica hint: %w", err)
	}

	if err := s.hintQueue.Enqueue(hint); err != nil {
		return fmt.Errorf("persist replica hint: %w", err)
	}

	log.Printf(
		"queued hint %s for key %d on node %s",
		hint.ID,
		key,
		node.ID,
	)

	return nil
}
