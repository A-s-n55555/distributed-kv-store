package membership

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestCandidateFileRoundTripAndTamperDetection(t *testing.T) {
	currentRing := ring.New(100)
	currentRing.AddNode(ring.Node{
		ID: "node-1", Address: "localhost:50051",
	})
	currentRing.AddNode(ring.Node{
		ID: "node-2", Address: "localhost:50052",
	})

	current, err := NewConfiguration(1, currentRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := PrepareJoin(current, ring.Node{
		ID: "node-3", Address: "localhost:50053",
	})
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "membership-candidate.json")
	if err := SaveCandidate(path, candidate); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadCandidate(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Identity() != candidate.Identity() ||
		len(loaded.Nodes()) != 3 {
		t.Fatalf("incorrect recovered candidate: %+v", loaded.Identity())
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Replace(data, []byte("node-3"), []byte("node-x"), 1)
	if bytes.Equal(tampered, data) {
		t.Fatal("test did not alter the file")
	}
	if err := os.WriteFile(path, tampered, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCandidate(path); err == nil {
		t.Fatal("accepted a candidate whose nodes no longer match its digest")
	}
}
