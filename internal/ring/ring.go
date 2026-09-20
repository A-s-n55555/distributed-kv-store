package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

type Node struct {
	ID      string
	Address string
}

type virtualNode struct {
	position uint32
	nodeID   string
}

type Ring struct {
	mu               sync.RWMutex
	virtualNodeCount int
	nodes            map[string]Node
	entries          []virtualNode
}

func New(virtualNodeCount int) *Ring {
	return &Ring{
		virtualNodeCount: virtualNodeCount,
		nodes:            make(map[string]Node),
	}
}

func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node.ID]; exists {
		return
	}

	r.nodes[node.ID] = node

	for i := 0; i < r.virtualNodeCount; i++ {
		position := hash(fmt.Sprintf("%s#%d", node.ID, i))

		r.entries = append(r.entries, virtualNode{
			position: position,
			nodeID:   node.ID,
		})
	}

	sort.Slice(r.entries, func(i, j int) bool {
		return r.entries[i].position < r.entries[j].position
	})
}

// NodeCount returns the number of distinct physical nodes in the ring.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.nodes)
}

// Configuration returns the virtual-node count and an independent,
// deterministically ordered copy of all physical nodes.
func (r *Ring) Configuration() (int, []Node) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]Node, 0, len(r.nodes))

	for _, node := range r.nodes {
		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].ID == nodes[j].ID {
			return nodes[i].Address < nodes[j].Address
		}

		return nodes[i].ID < nodes[j].ID
	})

	return r.virtualNodeCount, nodes
}

func (r *Ring) GetReplicas(
	key string,
	replicationFactor int,
) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 || replicationFactor <= 0 {
		return nil
	}

	// Replicas cannot exceed the number of physical nodes.
	if replicationFactor > len(r.nodes) {
		replicationFactor = len(r.nodes)
	}

	position := hash(key)

	startIndex := sort.Search(
		len(r.entries),
		func(i int) bool {
			return r.entries[i].position >= position
		},
	)

	// Wrap around to the beginning of the ring.
	if startIndex == len(r.entries) {
		startIndex = 0
	}

	replicas := make([]Node, 0, replicationFactor)

	// Used as a set to prevent selecting the same physical node twice.
	selectedNodeIDs := make(
		map[string]struct{},
		replicationFactor,
	)

	for offset := 0; offset < len(r.entries) &&
		len(replicas) < replicationFactor; offset++ {

		index := (startIndex + offset) % len(r.entries)
		entry := r.entries[index]

		if _, alreadySelected :=
			selectedNodeIDs[entry.nodeID]; alreadySelected {
			continue
		}

		node, exists := r.nodes[entry.nodeID]
		if !exists {
			continue
		}

		replicas = append(replicas, node)
		selectedNodeIDs[entry.nodeID] = struct{}{}
	}

	return replicas
}

func (r *Ring) GetNode(key string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 {
		return Node{}, false
	}

	position := hash(key)

	index := sort.Search(len(r.entries), func(i int) bool {
		return r.entries[i].position >= position
	})

	if index == len(r.entries) {
		index = 0
	}

	node := r.nodes[r.entries[index].nodeID]
	return node, true
}

func hash(value string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(value))

	return hasher.Sum32()
}
