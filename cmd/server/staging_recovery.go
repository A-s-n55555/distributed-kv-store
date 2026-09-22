package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
)

// loadStagingJoinState prefers durable promotion state over external files.
func loadStagingJoinState(
	nodeID, address, activeFile, candidateFile, pausePath string,
) (membership.Configuration, membership.Configuration, error) {
	current, candidate, err := membership.LoadStagingPromotion(
		pausePath, nodeID,
	)

	if errors.Is(err, os.ErrNotExist) {
		// Initial staging still requires the supplied configuration files.
		candidate, err = loadStagingConfiguration(
			nodeID, address, activeFile, candidateFile,
		)
		if err != nil {
			return membership.Configuration{}, membership.Configuration{}, err
		}

		current, err = membership.LoadCandidate(activeFile)
	}
	if err != nil {
		return membership.Configuration{}, membership.Configuration{},
			fmt.Errorf("load staging join state: %w", err)
	}

	joining, err := membership.ValidateJoinCandidate(current, candidate)
	if err != nil {
		return membership.Configuration{}, membership.Configuration{}, err
	}
	if joining.ID != nodeID || joining.Address != address {
		return membership.Configuration{}, membership.Configuration{},
			fmt.Errorf("staging node ID and address differ from saved join")
	}

	// Apply the same listener restriction during recovery as initial staging.
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return membership.Configuration{}, membership.Configuration{},
			fmt.Errorf("staging address: %w", err)
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") &&
		(ip == nil || !ip.IsLoopback()) {
		return membership.Configuration{}, membership.Configuration{},
			fmt.Errorf("staging address must be loopback")
	}

	return current, candidate, nil
}
