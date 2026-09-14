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
	mu       sync.RWMutex
	replicas int
	nodes    map[string]Node
	entries  []virtualNode
}

func New(replicas int) *Ring {
	return &Ring{
		replicas: replicas,
		nodes:    make(map[string]Node),
	}
}

func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node.ID]; exists {
		return
	}

	r.nodes[node.ID] = node

	for i := 0; i < r.replicas; i++ {
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
