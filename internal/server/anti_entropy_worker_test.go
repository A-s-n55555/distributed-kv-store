package server

import (
	"context"
	"testing"
	"time"
)

func TestAntiEntropyWorkerLifecycle(t *testing.T) {
	s := &GRPCServer{}

	if err := s.StartAntiEntropy(
		context.Background(),
		0,
	); err == nil {
		t.Fatal("accepted a zero anti-entropy interval")
	}

	if err := s.StartAntiEntropy(
		context.Background(),
		time.Hour,
	); err != nil {
		t.Fatalf("StartAntiEntropy() error = %v", err)
	}

	if err := s.StartAntiEntropy(
		context.Background(),
		time.Hour,
	); err == nil {
		t.Fatal("started a duplicate anti-entropy worker")
	}

	s.StopAntiEntropy()

	// A stopped worker must release its state so it can be started again.
	if err := s.StartAntiEntropy(
		context.Background(),
		time.Hour,
	); err != nil {
		t.Fatalf(
			"StartAntiEntropy() after StopAntiEntropy() error = %v",
			err,
		)
	}

	s.StopAntiEntropy()
}
