package index

import (
	"fmt"
	"sync"
	"testing"
)

// Standard mutex map for comparison
type MutexIndex struct {
	mu   sync.RWMutex
	data map[string]int64
}

func NewMutexIndex() *MutexIndex {
	return &MutexIndex{
		data: make(map[string]int64),
	}
}

func (m *MutexIndex) Put(key string, offset int64) {
	m.mu.Lock()
	m.data[key] = offset
	m.mu.Unlock()
}

func (m *MutexIndex) Get(key string) (int64, bool) {
	m.mu.RLock()
	val, ok := m.data[key]
	m.mu.RUnlock()
	return val, ok
}

func BenchmarkIndex_Put_Safemap(b *testing.B) {
	idx := NewMapIndex()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Put(fmt.Sprintf("key-%d", i), int64(i))
	}
}

func BenchmarkIndex_Put_MutexMap(b *testing.B) {
	idx := NewMutexIndex()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Put(fmt.Sprintf("key-%d", i), int64(i))
	}
}

func BenchmarkIndex_Get_Safemap_Parallel(b *testing.B) {
	idx := NewMapIndex()
	// Pre-populate
	for i := 0; i < 1000; i++ {
		idx.Put(fmt.Sprintf("key-%d", i), int64(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

func BenchmarkIndex_Get_MutexMap_Parallel(b *testing.B) {
	idx := NewMutexIndex()
	// Pre-populate
	for i := 0; i < 1000; i++ {
		idx.Put(fmt.Sprintf("key-%d", i), int64(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}
