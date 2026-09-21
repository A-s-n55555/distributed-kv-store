package membership

import (
	"testing"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func TestValidateJoinIntentChecksExactTransition(t *testing.T) {
	oldRing := ring.New(16)
	oldRing.AddNode(ring.Node{ID: "node-1", Address: "localhost:50051"})
	oldRing.AddNode(ring.Node{ID: "node-2", Address: "localhost:50052"})

	current, err := NewConfiguration(1, oldRing, 2)
	if err != nil {
		t.Fatal(err)
	}
	joining := ring.Node{ID: "node-3", Address: "localhost:50053"}
	candidate, err := PrepareJoin(current, joining)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ValidateJoinIntent(
		current, current.Identity(), joining, candidate.Identity(),
	)
	if err != nil || got.Identity() != candidate.Identity() {
		t.Fatalf("valid join: got %v, error %v", got.Identity(), err)
	}

	staleActive := current.Identity()
	staleActive.Epoch++
	if _, err := ValidateJoinIntent(
		current, staleActive, joining, candidate.Identity(),
	); err == nil {
		t.Fatal("accepted a different active epoch")
	}

	wrongCandidate := candidate.Identity()
	wrongCandidate.Digest[0] ^= 0xff
	if _, err := ValidateJoinIntent(
		current, current.Identity(), joining, wrongCandidate,
	); err == nil {
		t.Fatal("accepted a different candidate digest")
	}

	changedAddress := ring.Node{ID: "node-3", Address: "localhost:60053"}
	if _, err := ValidateJoinIntent(
		current, current.Identity(), changedAddress, candidate.Identity(),
	); err == nil {
		t.Fatal("accepted a changed joining-node address")
	}
}
