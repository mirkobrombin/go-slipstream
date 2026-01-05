package merkle

import (
	"crypto/sha256"
	"maps"
	"slices"
)

// Tree represents the state of a node.
type Tree struct {
	nodes map[string][32]byte
}

func New() *Tree {
	return &Tree{
		nodes: make(map[string][32]byte),
	}
}

func (m *Tree) Update(key string, data []byte) {
	m.nodes[key] = sha256.Sum256(data)
}

func (m *Tree) Delete(key string) {
	delete(m.nodes, key)
}

func (m *Tree) Root() [32]byte {
	if len(m.nodes) == 0 {
		return [32]byte{}
	}

	keys := slices.Collect(maps.Keys(m.nodes))
	slices.Sort(keys)

	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		hash := m.nodes[k]
		h.Write(hash[:])
	}

	var root [32]byte
	copy(root[:], h.Sum(nil))
	return root
}
