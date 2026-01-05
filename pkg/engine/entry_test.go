package engine

import (
	"context"
	"testing"

	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func TestSlipstream_Wrapper(t *testing.T) {
	ctx := context.Background()

	t.Run("Basic Ops", func(t *testing.T) {
		dir := t.TempDir()
		w, err := wal.NewManager(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		codec := func(s string) ([]byte, error) { return []byte(s), nil }
		decoder := func(b []byte) (string, error) { return string(b), nil }

		realEngine := New[string](w, codec, decoder)
		s := NewSlipstream[string](realEngine)

		_ = s.Put(ctx, "hello", "world", 0)
		val, _ := s.Get(ctx, "hello")
		if val != "world" {
			t.Errorf("Expected world, got %s", val)
		}

		_ = s.Delete(ctx, "hello")
		_, err = s.Get(ctx, "hello")
		if err == nil {
			t.Error("Expected error after delete")
		}

		_ = s.Close()
		err = s.Put(ctx, "fail", "it", 0)
		if err != ErrClosed {
			t.Errorf("Expected ErrClosed, got %v", err)
		}
	})
}

func TestBitcask_Entry(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	codec := func(s string) ([]byte, error) { return []byte(s), nil }
	decoder := func(b []byte) (string, error) { return string(b), nil }

	bc := NewBitcask[string](w, codec, decoder)
	ctx := context.Background()

	_ = bc.Put(ctx, "k1", "v1", 0)
	val, _ := bc.Get(ctx, "k1")
	if val != "v1" {
		t.Errorf("Got %s", val)
	}
}
