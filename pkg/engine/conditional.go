package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func (e *Engine[T]) PutIfAbsent(ctx context.Context, key string, value T, ttl time.Duration) error {
	raw, compressed, err := e.encodeValue(value)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists, err := e.currentLocked(key, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if exists {
		return ErrKeyExists
	}
	return e.putPreparedLocked(ctx, key, value, raw, compressed, expiration(ttl, time.Now()))
}

func (e *Engine[T]) PutIf(ctx context.Context, key string, value T, ttl time.Duration, condition tx.Condition[T]) error {
	if condition == nil {
		return tx.ErrInvalidCondition
	}
	raw, compressed, err := e.encodeValue(value)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	current, exists, err := e.currentLocked(key, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if !condition(current, exists) {
		return fmt.Errorf("%w: key %q", tx.ErrConditionFailed, key)
	}
	return e.putPreparedLocked(ctx, key, value, raw, compressed, expiration(ttl, time.Now()))
}

func (e *Engine[T]) DeleteIf(ctx context.Context, key string, condition tx.Condition[T]) error {
	if condition == nil {
		return tx.ErrInvalidCondition
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	current, exists, err := e.currentLocked(key, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if !condition(current, exists) {
		return fmt.Errorf("%w: key %q", tx.ErrConditionFailed, key)
	}
	if offset, ok := e.primary.Get(key); ok {
		if _, _, _, err := e.valueAt(offset, 0, false); err != nil {
			return err
		}
	}
	if _, err := e.wal.Append(wal.Entry{Type: wal.EntryDelete, Key: key}); err != nil {
		return err
	}
	return e.applyDeleteLocked(ctx, key)
}
