package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

type bufferedOp[T any] struct {
	typ        byte
	key        string
	value      T
	raw        []byte
	compressed []byte
	ttl        time.Duration
}

type requirement[T any] struct {
	key       string
	condition tx.Condition[T]
}

type btx[T any] struct {
	mu           sync.Mutex
	engine       *Engine[T]
	txID         uint64
	ops          []bufferedOp[T]
	requirements []requirement[T]
	done         bool
}

func (e *Engine[T]) beginTransaction() *btx[T] {
	return &btx[T]{engine: e, txID: e.wal.NextTxID()}
}

func (e *Engine[T]) BeginConditional() (tx.ConditionalTransaction[T], error) {
	return e.beginTransaction(), nil
}

func (b *btx[T]) Get(ctx context.Context, key string) (T, error) {
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		var zero T
		return zero, tx.ErrDone
	}
	for i := len(b.ops) - 1; i >= 0; i-- {
		op := b.ops[i]
		if op.key != key {
			continue
		}
		b.mu.Unlock()
		if op.typ == wal.EntryDelete {
			var zero T
			return zero, ErrKeyNotFound
		}
		return op.value, nil
	}
	b.mu.Unlock()
	return b.engine.Get(ctx, key)
}

func (b *btx[T]) Put(_ context.Context, key string, value T, ttl time.Duration) error {
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return tx.ErrDone
	}
	b.mu.Unlock()
	raw, compressed, err := b.engine.encodeValue(value)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return tx.ErrDone
	}
	b.ops = append(b.ops, bufferedOp[T]{
		typ:        wal.EntryPut,
		key:        key,
		value:      value,
		raw:        raw,
		compressed: compressed,
		ttl:        ttl,
	})
	return nil
}

func (b *btx[T]) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return tx.ErrDone
	}
	b.ops = append(b.ops, bufferedOp[T]{typ: wal.EntryDelete, key: key})
	return nil
}

func (b *btx[T]) Require(key string, condition tx.Condition[T]) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return tx.ErrDone
	}
	if condition == nil {
		return tx.ErrInvalidCondition
	}
	b.requirements = append(b.requirements, requirement[T]{key: key, condition: condition})
	return nil
}

func (b *btx[T]) Commit(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return tx.ErrDone
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.engine.mu.Lock()
	defer b.engine.mu.Unlock()
	now := time.Now()
	for _, requirement := range b.requirements {
		current, exists, err := b.engine.currentLocked(requirement.key, now.UnixNano())
		if err != nil {
			return err
		}
		if !requirement.condition(current, exists) {
			b.done = true
			return fmt.Errorf("%w: key %q", tx.ErrConditionFailed, requirement.key)
		}
	}

	checked := make(map[string]struct{})
	for _, op := range b.ops {
		if _, ok := checked[op.key]; ok {
			continue
		}
		checked[op.key] = struct{}{}
		offset, ok := b.engine.primary.Get(op.key)
		if !ok {
			continue
		}
		if _, _, _, err := b.engine.valueAt(offset, 0, false); err != nil {
			return err
		}
	}

	type writtenOp struct {
		entry  wal.Entry
		offset int64
	}
	written := make([]writtenOp, 0, len(b.ops))
	overlay := make(map[uint64]int64)
	for _, op := range b.ops {
		if op.typ == wal.EntryDelete {
			entry := wal.Entry{Type: wal.EntryDelete, TxID: b.txID, Key: op.key}
			offset, err := b.engine.wal.Append(entry)
			if err != nil {
				b.done = true
				return err
			}
			written = append(written, writtenOp{entry: entry, offset: offset})
			continue
		}
		entry, offset, err := b.engine.appendPutLocked(op.key, op.compressed, expiration(op.ttl, now), b.txID, overlay)
		if err != nil {
			b.done = true
			return err
		}
		written = append(written, writtenOp{entry: entry, offset: offset})
	}
	if _, err := b.engine.wal.Append(wal.Entry{Type: wal.EntryCommit, TxID: b.txID}); err != nil {
		b.done = true
		return fmt.Errorf("%w: %v", tx.ErrCommitUncertain, err)
	}
	if err := b.engine.wal.Sync(); err != nil {
		b.done = true
		return fmt.Errorf("%w: %v", tx.ErrCommitUncertain, err)
	}

	for hash, offset := range overlay {
		b.engine.dedup[hash] = offset
	}
	for i, op := range b.ops {
		var err error
		if op.typ == wal.EntryDelete {
			err = b.engine.applyDeleteLocked(ctx, op.key)
		} else {
			err = b.engine.applyPutLocked(ctx, op.key, written[i].offset, op.value, op.raw)
			if b.engine.dedupEnabled && written[i].entry.Type == wal.EntryPut {
				b.engine.dedup[xxhash.Sum64(op.compressed)] = written[i].offset
			}
		}
		if err != nil {
			b.done = true
			return fmt.Errorf("%w: %v", tx.ErrCommitUncertain, err)
		}
	}
	b.done = true
	b.ops = nil
	b.requirements = nil
	return nil
}

func (b *btx[T]) Rollback() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return tx.ErrDone
	}
	b.done = true
	b.ops = nil
	b.requirements = nil
	return nil
}
