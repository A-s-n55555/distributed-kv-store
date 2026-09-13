package main

import (
	"fmt"
	"sync"

	// "golang.org/x/tools/go/analysis/passes/defers"
)

type Map struct {
	mu sync.RWMutex
	data map[int]string
}

func NewMap() *Map {
	return &Map{
		data: make(map[int]string),
	}
}

func (m *Map) Put(key int, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *Map) Get(key int) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists := m.data[key]
	return value, exists
}

func (m *Map) Delete(key int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func main(){
	fmt.Println("this is out first key-value store");
	var map1 = NewMap();
	map1.Put(1, "value1");
	fmt.Println(map1.Get(1));
	map1.Delete(1);
	fmt.Println(map1.Get(1));
}