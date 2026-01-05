package warp

import (
	"context"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-warp/v1/adapter"
)

// Store is a go-warp adapter for go-slipstream Engine.
type Store[T any] struct {
	engine *engine.Engine[T]
}

// NewStore returns a new Store adapter.
func NewStore[T any](e *engine.Engine[T]) *Store[T] {
	return &Store[T]{engine: e}
}

// Get implements adapter.Store.Get.
func (s *Store[T]) Get(ctx context.Context, key string) (T, bool, error) {
	val, err := s.engine.Get(ctx, key)
	if err != nil {
		var zero T
		// Mapping Slipstream not found to false
		return zero, false, nil
	}
	return val, true, nil
}

// Set implements adapter.Store.Set.
func (s *Store[T]) Set(ctx context.Context, key string, value T) error {
	return s.engine.Put(ctx, key, value, 0)
}

// Keys implements adapter.Store.Keys.
func (s *Store[T]) Keys(ctx context.Context) ([]string, error) {
	var keys []string
	err := s.engine.ForEach(func(key string, val T) error {
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

// Batch implements adapter.Batcher.Batch.
func (s *Store[T]) Batch(ctx context.Context) (adapter.Batch[T], error) {
	tx, err := s.engine.Begin()
	if err != nil {
		return nil, err
	}
	return &batch[T]{tx: tx}, nil
}

type batch[T any] struct {
	tx interface {
		Put(ctx context.Context, key string, value T, ttl time.Duration) error
		Delete(ctx context.Context, key string) error
		Commit(ctx context.Context) error
	}
}

func (b *batch[T]) Set(ctx context.Context, key string, value T) error {
	return b.tx.Put(ctx, key, value, 0)
}

func (b *batch[T]) Delete(ctx context.Context, key string) error {
	return b.tx.Delete(ctx, key)
}

func (b *batch[T]) Commit(ctx context.Context) error {
	return b.tx.Commit(ctx)
}

// Ensure interface implementation
var _ adapter.Store[any] = (*Store[any])(nil)
var _ adapter.Batcher[any] = (*Store[any])(nil)
