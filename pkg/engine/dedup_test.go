package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func TestStorage_Deduplication(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewManager(dir)

	codec := func(s string) ([]byte, error) { return []byte(s), nil }
	decoder := func(b []byte) (string, error) { return string(b), nil }

	e := New[string](w, codec, decoder)
	e.EnableDeduplication(true)

	largeValue := "this is a very large value that should be deduplicated "
	for i := 0; i < 10; i++ {
		largeValue += largeValue
	}

	ctx := context.Background()

	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		if err := e.Put(ctx, key, largeValue, 0); err != nil {
			t.Fatal(err)
		}
	}

	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		val, _ := e.Get(ctx, key)
		if val != largeValue {
			t.Errorf("mismatch for %s", key)
		}
	}

	off0, _ := e.primary.Get("key-0")
	off1, _ := e.primary.Get("key-1")

	_, o0 := wal.UnpackOffset(off0)
	_, o1 := wal.UnpackOffset(off1)

	diff := o1 - o0
	if int(diff) < len(largeValue) {
		t.Logf("Deduplication working: diff between entries is %d, while value is %d", diff, len(largeValue))
	} else {
		t.Errorf("Deduplication MIGHT NOT be working: diff=%d, valSize=%d", diff, len(largeValue))
	}
}
