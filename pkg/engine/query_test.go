package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func TestQuery_FluentAPI(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewManager(dir)

	type Post struct {
		ID       string
		Category string
		Views    int
	}

	codec := func(p Post) ([]byte, error) {
		return []byte(fmt.Sprintf("%s|%s|%d", p.ID, p.Category, p.Views)), nil
	}
	decoder := func(b []byte) (Post, error) {
		parts := strings.Split(string(b), "|")
		if len(parts) < 3 {
			return Post{}, fmt.Errorf("invalid post data")
		}
		var views int
		fmt.Sscanf(parts[2], "%d", &views)
		return Post{ID: parts[0], Category: parts[1], Views: views}, nil
	}

	e := New[Post](w, codec, decoder)
	e.AddIndex("cat", func(p Post) string { return p.Category })

	ctx := context.Background()
	e.Put(ctx, "p1", Post{"p1", "tech", 100}, 0)
	e.Put(ctx, "p2", Post{"p2", "tech", 500}, 0)
	e.Put(ctx, "p3", Post{"p3", "life", 200}, 0)
	e.Put(ctx, "p4", Post{"p4", "tech", 300}, 0)

	t.Run("FilterSortLimit", func(t *testing.T) {
		res, err := e.GetByIndex(ctx, "cat", "tech").
			Filter(func(p Post) bool { return p.Views > 150 }).
			Sort(func(i, j Post) bool { return i.Views > j.Views }).
			Limit(1).
			All()

		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(res) != 1 {
			t.Fatalf("expected 1 result, got %d", len(res))
		}

		if res[0].ID != "p2" {
			t.Errorf("expected p2 (highest views), got %s", res[0].ID)
		}
	})
}
