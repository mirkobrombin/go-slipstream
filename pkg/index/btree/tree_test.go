package btree

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestBTree_Basic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "btree.db")
	tree, err := New(path)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer tree.Close()

	// Insert
	keys := []string{"alpha", "beta", "gamma", "delta"}
	for i, k := range keys {
		err := tree.Put([]byte(k), int64(i*100))
		if err != nil {
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	// Get
	for i, k := range keys {
		val, ok := tree.Get([]byte(k))
		if !ok {
			t.Errorf("Get %s not found", k)
		}
		if val != int64(i*100) {
			t.Errorf("Get %s = %d, want %d", k, val, i*100)
		}
	}

	// Not found
	if _, ok := tree.Get([]byte("missing")); ok {
		t.Error("Found missing key")
	}
}

func TestBTree_Split(t *testing.T) {
	path := filepath.Join(t.TempDir(), "btree_split.db")
	tree, err := New(path)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer tree.Close()

	count := 1000
	for i := range count {
		k := fmt.Sprintf("key-%04d", i)
		err := tree.Put([]byte(k), int64(i))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Verify
	for i := range count {
		k := fmt.Sprintf("key-%04d", i)
		val, ok := tree.Get([]byte(k))
		if !ok {
			t.Errorf("Get %s not found", k)
		}
		if val != int64(i) {
			t.Errorf("Val mismatch for %s: got %d", k, val)
		}
	}
}

func TestBTree_Persistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "btree_persist.db")
	tree, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	tree.Put([]byte("persistent"), 12345)
	tree.Close()

	// Reopen
	tree2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer tree2.Close()

	val, ok := tree2.Get([]byte("persistent"))
	if !ok {
		t.Error("Key lost persistence")
	}
	if val != 12345 {
		t.Errorf("Value mismatch: %d", val)
	}
}
