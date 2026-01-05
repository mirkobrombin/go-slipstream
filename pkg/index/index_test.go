package index

import (
	"path/filepath"
	"testing"
)

func TestHybridIndex_Spill(t *testing.T) {
	tmpDir := t.TempDir()
	tmpCold := filepath.Join(tmpDir, "test_hybrid_cold.idx")

	h := NewHybridIndex(tmpCold, 2)

	h.Put("k1", 100)
	h.Put("k2", 200)

	if len(h.hot) != 2 {
		t.Errorf("Expected 2 keys in RAM, got %d", len(h.hot))
	}

	h.Put("k3", 300)

	if len(h.hot) > 2 {
		t.Errorf("Expected hot cache to be <= 2 after spill, got %d", len(h.hot))
	}

	off, ok := h.Get("k1")
	if !ok {
		t.Error("Expected to find k1 in either hot or cold storage")
	}
	_ = off

	total := 0
	h.ForEach(func(k string, v int64) error {
		total++
		return nil
	})
}
