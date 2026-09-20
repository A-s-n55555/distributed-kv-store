package server

import (
	"context"
	"fmt"
	"log"
	"time"
)

// StartAntiEntropy begins one periodic anti-entropy worker for this node.
// It performs one synchronization immediately, then repeats after interval.
func (s *GRPCServer) StartAntiEntropy(
	parent context.Context,
	interval time.Duration,
) error {
	if parent == nil {
		return fmt.Errorf("anti-entropy parent context is required")
	}

	if interval <= 0 {
		return fmt.Errorf("anti-entropy interval must be positive")
	}

	s.antiEntropyMu.Lock()
	defer s.antiEntropyMu.Unlock()

	if s.antiEntropyCancel != nil {
		return fmt.Errorf("anti-entropy worker is already running")
	}

	workerContext, cancel := context.WithCancel(parent)
	done := make(chan struct{})

	s.antiEntropyCancel = cancel
	s.antiEntropyDone = done

	go func() {
		defer func() {
			s.antiEntropyMu.Lock()
			s.antiEntropyCancel = nil
			s.antiEntropyDone = nil
			close(done)
			s.antiEntropyMu.Unlock()
		}()

		s.syncAllAntiEntropyPeers(workerContext)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-workerContext.Done():
				return

			case <-ticker.C:
				s.syncAllAntiEntropyPeers(workerContext)
			}
		}
	}()

	return nil
}

// StopAntiEntropy stops the worker and waits until it exits.
func (s *GRPCServer) StopAntiEntropy() {
	s.antiEntropyMu.Lock()
	cancel := s.antiEntropyCancel
	done := s.antiEntropyDone
	s.antiEntropyMu.Unlock()

	if cancel == nil {
		return
	}

	cancel()
	<-done
}

// syncAllAntiEntropyPeers performs one pull from every other static member.
func (s *GRPCServer) syncAllAntiEntropyPeers(
	ctx context.Context,
) {
	if s.ring == nil {
		log.Printf("anti-entropy skipped: cluster ring is unavailable")
		return
	}

	_, nodes := s.ring.Configuration()

	for _, peer := range nodes {
		if peer.ID == s.nodeID {
			continue
		}

		updated, err := s.syncAntiEntropyFromPeer(ctx, peer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf(
				"anti-entropy sync from %s failed: %v",
				peer.ID,
				err,
			)
			continue
		}

		if updated > 0 {
			log.Printf(
				"anti-entropy applied %d versions from %s",
				updated,
				peer.ID,
			)
		}
	}
}
