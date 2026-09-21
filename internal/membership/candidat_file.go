package membership

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/A-s-n55555/distributed-kv-store/internal/antientropy"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

type candidateFile struct {
	Epoch             uint64      `json:"epoch"`
	VirtualNodeCount  int         `json:"virtual_node_count"`
	ReplicationFactor int         `json:"replication_factor"`
	Nodes             []ring.Node `json:"nodes"`
	Digest            string      `json:"digest"`
}

// SaveCandidate atomically replaces a proposal file. It does not activate it.
// The parent directory must already exist.
func SaveCandidate(path string, candidate Configuration) error {
	if path == "" {
		return fmt.Errorf("candidate path is required")
	}
	if candidate.identity.Epoch == 0 {
		return fmt.Errorf("candidate configuration is invalid")
	}

	digest, err := antientropy.ConfigurationDigest(
		candidate.virtualNodeCount,
		candidate.replicationFactor,
		candidate.nodes,
	)
	if err != nil {
		return err
	}
	if digest != candidate.identity.Digest {
		return fmt.Errorf("candidate configuration digest does not match")
	}

	data, err := json.Marshal(candidateFile{
		Epoch:             candidate.identity.Epoch,
		VirtualNodeCount:  candidate.virtualNodeCount,
		ReplicationFactor: candidate.replicationFactor,
		Nodes:             candidate.Nodes(),
		Digest:            hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return err
	}

	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".membership-candidate-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}

	// Make the rename durable on filesystems that support directory sync.
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// LoadCandidate validates the saved identity before rebuilding its ring.
func LoadCandidate(path string) (Configuration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Configuration{}, err
	}

	var saved candidateFile
	if err := json.Unmarshal(data, &saved); err != nil {
		return Configuration{}, err
	}
	if saved.Epoch == 0 {
		return Configuration{}, fmt.Errorf("saved epoch must be positive")
	}

	digest, err := antientropy.ConfigurationDigest(
		saved.VirtualNodeCount,
		saved.ReplicationFactor,
		saved.Nodes,
	)
	if err != nil {
		return Configuration{}, err
	}
	if saved.Digest != hex.EncodeToString(digest[:]) {
		return Configuration{}, fmt.Errorf("saved configuration digest does not match")
	}

	// Validate the raw list before AddNode, which ignores repeated IDs.
	addresses := make(map[string]bool, len(saved.Nodes))
	for _, node := range saved.Nodes {
		if addresses[node.Address] {
			return Configuration{}, fmt.Errorf(
				"address %q appears more than once", node.Address,
			)
		}
		addresses[node.Address] = true
	}

	proposedRing := ring.New(saved.VirtualNodeCount)
	for _, node := range saved.Nodes {
		proposedRing.AddNode(node)
	}
	return NewConfiguration(
		saved.Epoch, proposedRing, saved.ReplicationFactor,
	)
}