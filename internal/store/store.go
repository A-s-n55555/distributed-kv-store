package main
import "fmt"

type Map struct {
	data map[int]string
}

func NewMap() *Map {
	return &Map{
		data: make(map[int]string),
	}
}

func (m *Map) Put(key int, value string) {
	m.data[key] = value
}

func (m *Map) Get(key int) string {
	return m.data[key]
}

func (m *Map) Delete(key int) {
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