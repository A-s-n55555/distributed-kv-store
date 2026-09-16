package version

// Clock records the write counter associated with each node.
type Clock map[string]uint64

// Relation describes the relationship between two clocks.
type Relation int

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

// Clone creates an independent copy of a clock.
func Clone(clock Clock) Clock {
	result := make(Clock, len(clock))

	for nodeID, counter := range clock {
		result[nodeID] = counter
	}

	return result
}

// Increment advances one node's counter without modifying the input.
func Increment(clock Clock, nodeID string) Clock {
	result := Clone(clock)
	result[nodeID]++

	return result
}

// Merge combines the knowledge contained in two clocks.
// Each node receives the maximum counter observed for that node.
func Merge(first, second Clock) Clock {
	result := Clone(first)

	for nodeID, counter := range second {
		if counter > result[nodeID] {
			result[nodeID] = counter
		}
	}

	return result
}

// Compare determines whether the first clock is equal to,
// before, after, or concurrent with the second clock.
// Missing node counters are treated as zero.
func Compare(first, second Clock) Relation {
	hasSmaller := false
	hasGreater := false

	for nodeID, counter := range first {
		otherCounter := second[nodeID]

		if counter < otherCounter {
			hasSmaller = true
		}

		if counter > otherCounter {
			hasGreater = true
		}
	}

	for nodeID, counter := range second {
		if _, exists := first[nodeID]; !exists && counter > 0 {
			hasSmaller = true
		}
	}

	switch {
	case hasSmaller && hasGreater:
		return Concurrent
	case hasSmaller:
		return Before
	case hasGreater:
		return After
	default:
		return Equal
	}
}
