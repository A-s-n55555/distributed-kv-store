package main

import (
	"fmt"
	"log"
	"os"

	"github.com/A-s-n55555/distributed-kv-store/internal/membership"
	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
)

func main() {
	const previousPath = "data/join/previous.json"
	const candidatePath = "data/join/candidate.json"

	if _, err := os.Stat(candidatePath); err == nil {
		log.Fatalf("%s already exists; keep it for join retries", candidatePath)
	} else if !os.IsNotExist(err) {
		log.Fatal(err)
	}

	previous, err := membership.LoadCandidate(previousPath)
	if err != nil {
		log.Fatalf("load previous membership: %v", err)
	}

	candidate, err := membership.PrepareJoin(previous, ring.Node{
		ID:      "node-3",
		Address: "localhost:50053",
	})
	if err != nil {
		log.Fatalf("prepare join: %v", err)
	}

	if err := membership.SaveCandidate(candidatePath, candidate); err != nil {
		log.Fatalf("save candidate: %v", err)
	}

	fmt.Printf("saved %s (epoch %d)\n",
		candidatePath, candidate.Identity().Epoch)
}
