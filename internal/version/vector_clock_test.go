package version

import (
	"reflect"
	"testing"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name   string
		first  Clock
		second Clock
		want   Relation
	}{
		{
			name:   "equal",
			first:  Clock{"node-1": 2},
			second: Clock{"node-1": 2},
			want:   Equal,
		},
		{
			name:   "before",
			first:  Clock{"node-1": 1},
			second: Clock{"node-1": 2},
			want:   Before,
		},
		{
			name:   "after",
			first:  Clock{"node-1": 2},
			second: Clock{"node-1": 1},
			want:   After,
		},
		{
			name:   "concurrent writes",
			first:  Clock{"node-1": 1},
			second: Clock{"node-2": 1},
			want:   Concurrent,
		},
		{
			name: "mixed counters",
			first: Clock{
				"node-1": 2,
				"node-2": 1,
			},
			second: Clock{
				"node-1": 1,
				"node-2": 2,
			},
			want: Concurrent,
		},
		{
			name:   "missing counter means zero",
			first:  Clock{},
			second: Clock{"node-1": 0},
			want:   Equal,
		},
		{
			name:   "empty clock before a write",
			first:  nil,
			second: Clock{"node-1": 1},
			want:   Before,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Compare(test.first, test.second)

			if got != test.want {
				t.Fatalf(
					"Compare() = %v; want %v",
					got,
					test.want,
				)
			}
		})
	}
}

func TestIncrementDoesNotModifyOriginal(t *testing.T) {
	original := Clock{"node-1": 1}

	result := Increment(original, "node-1")

	if result["node-1"] != 2 {
		t.Fatalf("counter = %d; want 2", result["node-1"])
	}

	if original["node-1"] != 1 {
		t.Fatal("Increment() modified the original clock")
	}
}

func TestIncrementEmptyClock(t *testing.T) {
	result := Increment(nil, "node-1")

	if result["node-1"] != 1 {
		t.Fatalf("counter = %d; want 1", result["node-1"])
	}
}

func TestMerge(t *testing.T) {
	first := Clock{
		"node-1": 2,
		"node-2": 1,
	}

	second := Clock{
		"node-1": 1,
		"node-2": 3,
		"node-3": 1,
	}

	result := Merge(first, second)

	want := Clock{
		"node-1": 2,
		"node-2": 3,
		"node-3": 1,
	}

	if !reflect.DeepEqual(result, want) {
		t.Fatalf("Merge() = %v; want %v", result, want)
	}

	if first["node-2"] != 1 {
		t.Fatal("Merge() modified the original clock")
	}
}
