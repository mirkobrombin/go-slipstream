package adapter

import (
	"context"

	"github.com/mirkobrombin/go-slipstream/pkg/engine"
)

// WarpAdapter is a wrapper around the engine to provide Warp compatibility.
type WarpAdapter[T any] struct {
	engine *engine.Engine[T]
}

// NewWarpAdapter creates a new WarpAdapter.
func NewWarpAdapter[T any](e *engine.Engine[T]) *WarpAdapter[T] {
	return &WarpAdapter[T]{engine: e}
}

// Get retrieves a value from engine for Warp.
func (a *WarpAdapter[T]) Get(ctx context.Context, key string) (T, error) {
	return a.engine.Get(ctx, key)
}

// Set stores a value in engine for Warp.
func (a *WarpAdapter[T]) Set(ctx context.Context, key string, value T) error {
	return a.engine.Put(ctx, key, value, 0)
}

// Delete removes a value from engine for Warp.
func (a *WarpAdapter[T]) Delete(ctx context.Context, key string) error {
	return a.engine.Delete(ctx, key)
}
