package bloom

import (
	"hash/fnv"
)

// Filter is a probabilistic data structure.
type Filter struct {
	bitset []bool
	k      int
}

// New creates a new Bloom Filter.
func New(size int, k int) *Filter {
	if k <= 0 {
		k = 1
	}
	return &Filter{
		bitset: make([]bool, size),
		k:      k,
	}
}

func (b *Filter) Add(key string) {
	for i := 0; i < b.k; i++ {
		idx := b.hash(key, i) % uint64(len(b.bitset))
		b.bitset[idx] = true
	}
}

func (b *Filter) MayContain(key string) bool {
	for i := 0; i < b.k; i++ {
		idx := b.hash(key, i) % uint64(len(b.bitset))
		if !b.bitset[idx] {
			return false
		}
	}
	return true
}

func (b *Filter) hash(key string, seed int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	val := h.Sum64()
	return val + uint64(seed)*0x9e3779b9
}
