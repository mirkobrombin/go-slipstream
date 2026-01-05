package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func TestV1_FullFlow(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	codec := func(s string) ([]byte, error) { return []byte(s), nil }
	decoder := func(b []byte) (string, error) { return string(b), nil }

	e := New[string](w, codec, decoder)
	ctx := context.Background()

	t.Run("Basic Put/Get", func(t *testing.T) {
		_ = e.Put(ctx, "k1", "v1", 0)
		val, _ := e.Get(ctx, "k1")
		if val != "v1" {
			t.Errorf("Expected v1, got %s", val)
		}
	})

	t.Run("Transactions", func(t *testing.T) {
		tx, _ := e.Begin()
		_ = tx.Put(ctx, "tx1", "val1", 0)
		_ = tx.Commit(ctx)

		val, _ := e.Get(ctx, "tx1")
		if val != "val1" {
			t.Errorf("Expected val1, got %s", val)
		}
	})

	t.Run("Compaction", func(t *testing.T) {
		w.SetMaxSegmentSize(100)

		for i := range 20 {
			_ = e.Put(ctx, fmt.Sprintf("k%d", i), "value", 0)
		}

		for i := range 20 {
			_ = e.Put(ctx, fmt.Sprintf("k%d", i), "value-updated", 0)
		}

		if len(w.SealedSegments()) == 0 {
			t.Log("Warning: No sealed segments generated, compaction test might be vacuously true")
		}

		err := e.Compact()
		if err != nil {
			t.Fatalf("Compact failed: %v", err)
		}

		val, _ := e.Get(ctx, "k0")
		if val != "value-updated" {
			t.Errorf("Expected value-updated, got %s", val)
		}
	})

	t.Run("Secondary Index", func(t *testing.T) {
		type User struct {
			ID   string
			City string
		}
		uCodec := func(u User) ([]byte, error) { return []byte(u.ID + "|" + u.City), nil }
		uDec := func(b []byte) (User, error) { return User{ID: "u1", City: "Torino"}, nil }

		ue := New[User](w, uCodec, uDec)
		ue.AddIndex("city", func(u User) string { return u.City })

		_ = ue.Put(ctx, "u1", User{ID: "u1", City: "Torino"}, 0)
		res, _ := ue.GetByIndex(ctx, "city", "Torino").All()
		if len(res) == 0 {
			t.Error("Expected results in Torino")
		}
	})
}
