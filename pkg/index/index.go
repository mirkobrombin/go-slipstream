package index

import (
	"sync"

	"github.com/mirkobrombin/go-slipstream/pkg/index/btree"
)

// Indexer defines the interface for primary and secondary indexing.
type Indexer interface {
	Put(key string, offset int64)
	Get(key string) (int64, bool)
	Delete(key string)
	ForEach(func(key string, offset int64) error) error
	CompareAndSwap(key string, oldVal, newVal int64) bool
}

// MapIndex is a simple in-memory implementation of Indexer.
type MapIndex struct {
	mu   sync.RWMutex
	data map[string]int64
}

func NewMapIndex() *MapIndex {
	return &MapIndex{
		data: make(map[string]int64),
	}
}

func (m *MapIndex) Put(key string, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = offset
}

func (m *MapIndex) Get(key string) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	off, ok := m.data[key]
	return off, ok
}

func (m *MapIndex) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *MapIndex) ForEach(fn func(key string, offset int64) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MapIndex) CompareAndSwap(key string, oldVal, newVal int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.data[key]
	if !ok || current != oldVal {
		return false
	}
	m.data[key] = newVal
	return true
}

// SecondaryIndex manages inverted lookups.
type SecondaryIndex[T any] struct {
	mu         sync.RWMutex
	indices    map[string]map[string]map[string]struct{} // name -> val -> set[pk]
	extractors map[string]func(T) string
}

func NewSecondaryIndex[T any]() *SecondaryIndex[T] {
	return &SecondaryIndex[T]{
		indices:    make(map[string]map[string]map[string]struct{}),
		extractors: make(map[string]func(T) string),
	}
}

func (s *SecondaryIndex[T]) AddIndex(name string, extractor func(T) string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extractors[name] = extractor
	s.indices[name] = make(map[string]map[string]struct{})
}

func (s *SecondaryIndex[T]) Update(key string, val T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, extractor := range s.extractors {
		fieldVal := extractor(val)
		if s.indices[name][fieldVal] == nil {
			s.indices[name][fieldVal] = make(map[string]struct{})
		}
		s.indices[name][fieldVal][key] = struct{}{}
	}
}

func (s *SecondaryIndex[T]) RemoveEntry(name string, fieldVal string, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.indices[name]; ok {
		if _, ok := s.indices[name][fieldVal]; ok {
			delete(s.indices[name][fieldVal], key)
		}
	}
}

func (s *SecondaryIndex[T]) Get(name string, fieldVal string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keyMap, ok := s.indices[name]
	if !ok {
		return nil
	}
	pks, ok := keyMap[fieldVal]
	if !ok {
		return nil
	}
	results := make([]string, 0, len(pks))
	for pk := range pks {
		results = append(results, pk)
	}
	return results
}

func (s *SecondaryIndex[T]) Extractors() map[string]func(T) string {
	return s.extractors
}

// HybridIndex is an adaptive indexer that spills to disk when memory limit is reached.
type HybridIndex struct {
	mu        sync.RWMutex
	hot       map[string]int64
	cold      *btree.BTree
	threshold int
}

func NewHybridIndex(coldPath string, threshold int) *HybridIndex {
	cold, _ := btree.New(coldPath)
	return &HybridIndex{
		hot:       make(map[string]int64),
		cold:      cold,
		threshold: threshold,
	}
}

func (h *HybridIndex) Put(key string, offset int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hot[key] = offset
	if len(h.hot) > h.threshold {
		h.spill()
	}
}

func (h *HybridIndex) Get(key string) (int64, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	off, ok := h.hot[key]
	if ok {
		return off, true
	}

	if h.cold != nil {
		return h.cold.Get([]byte(key))
	}

	return 0, false
}

func (h *HybridIndex) spill() {
	for k, v := range h.hot {
		if h.cold != nil {
			_ = h.cold.Put([]byte(k), v)
		}
		delete(h.hot, k)
		break
	}
}

func (h *HybridIndex) Delete(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.hot, key)
}

func (h *HybridIndex) ForEach(fn func(key string, offset int64) error) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for k, v := range h.hot {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	// Also iterate disk overflow (stubbed)
	return nil
}

func (h *HybridIndex) spillToDisk() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cold == nil {
		return
	}

	for k, v := range h.hot {
		_ = h.cold.Put([]byte(k), v)
	}
	// Clear hot map after spilling
	h.hot = make(map[string]int64)
}

func (h *HybridIndex) CompareAndSwap(key string, oldVal, newVal int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check hot first
	if val, ok := h.hot[key]; ok {
		if val == oldVal {
			h.hot[key] = newVal
			return true
		}
		return false
	}

	// Check cold
	if h.cold != nil {
		val, ok := h.cold.Get([]byte(key))
		if ok && val == oldVal {
			// Update cold (or bring back to hot?)
			// Let's update in cold for now.
			err := h.cold.Put([]byte(key), newVal)
			return err == nil
		}
	}
	return false
}
