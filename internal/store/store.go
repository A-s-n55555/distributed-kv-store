package store

import (
	"fmt"
	"github.com/A-s-n55555/distributed-kv-store/internal/wal"
	"sync"
	// "golang.org/x/tools/go/analysis/passes/defers"
)

type Map struct {
	mu   sync.RWMutex
	data map[int]string
	wal  *wal.Log
}

func NewMap(log *wal.Log) (*Map, error) {
	entries, err := log.ReadAll()
	if err != nil {
		return nil, err
	}

	m := &Map{
		data: make(map[int]string),
		wal:  log,
	}

	for _, entry := range entries {
		switch entry.Operation {
		case "PUT":
			m.data[entry.Key] = entry.Value

		case "DELETE":
			delete(m.data, entry.Key)

		default:
			return nil, fmt.Errorf(
				"unknown WAL operation: %q",
				entry.Operation,
			)
		}
	}

	return m, nil
}

func (m *Map) Put(key int, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.wal.Append(wal.Entry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
	})
	if err != nil {
		return err
	}

	m.data[key] = value
	return nil
}

func (m *Map) Get(key int) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[key]
	return value, exists
}

func (m *Map) Delete(key int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.wal.Append(wal.Entry{
		Operation: "DELETE",
		Key:       key,
	})
	if err != nil {
		return err
	}

	delete(m.data, key)
	return nil
}

// func main(){
// 	fmt.Println("this is out first key-value store");
// 	var map1 = NewMap();
// 	map1.Put(1, "value1");
// 	fmt.Println(map1.Get(1));
// 	map1.Delete(1);
// 	fmt.Println(map1.Get(1));
// }
