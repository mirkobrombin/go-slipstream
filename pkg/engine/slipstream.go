package engine

import (
	"context"
	"sync"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/tx"
)

// Slipstream is a thread-safe wrapper that manages persistence.
type Slipstream[T any] struct {
	mu     sync.RWMutex
	closed bool
	engine *Engine[T]
}

// NewSlipstream creates a new Slipstream instance.
func NewSlipstream[T any](e *Engine[T]) *Slipstream[T] {
	return &Slipstream[T]{
		engine: e,
	}
}

func (s *Slipstream[T]) Get(ctx context.Context, key string) (T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		var zero T
		return zero, ErrClosed
	}

	return s.engine.Get(ctx, key)
}

func (s *Slipstream[T]) GetByIndex(ctx context.Context, indexName string, value string) *Result[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return &Result[T]{err: ErrClosed}
	}

	return s.engine.GetByIndex(ctx, indexName, value)
}
func (s *Slipstream[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	return s.engine.Put(ctx, key, value, ttl)
}

func (s *Slipstream[T]) PutIfAbsent(ctx context.Context, key string, value T, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.engine.PutIfAbsent(ctx, key, value, ttl)
}

func (s *Slipstream[T]) PutIf(ctx context.Context, key string, value T, ttl time.Duration, condition tx.Condition[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.engine.PutIf(ctx, key, value, ttl, condition)
}

func (s *Slipstream[T]) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	return s.engine.Delete(ctx, key)
}

func (s *Slipstream[T]) DeleteIf(ctx context.Context, key string, condition tx.Condition[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.engine.DeleteIf(ctx, key, condition)
}

func (s *Slipstream[T]) Begin() (tx.Transaction[T], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return s.engine.Begin()
}

func (s *Slipstream[T]) BeginConditional() (tx.ConditionalTransaction[T], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return s.engine.BeginConditional()
}

func (s *Slipstream[T]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.engine.Close()
}

func (s *Slipstream[T]) MerkleRoot() [32]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return [32]byte{}
	}

	return s.engine.MerkleRoot()
}
