package engine

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/mirkobrombin/go-foundation/v2/core/options"
	"github.com/mirkobrombin/go-slipstream/pkg/bloom"
	"github.com/mirkobrombin/go-slipstream/pkg/index"
	"github.com/mirkobrombin/go-slipstream/pkg/merkle"
	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
	"github.com/mirkobrombin/go-warp/v1/cache"
)

var (
	ErrClosed          = errors.New("slipstream: storage is closed")
	ErrKeyNotFound     = errors.New("slipstream: key not found")
	ErrKeyExists       = errors.New("slipstream: key already exists")
	ErrConditionFailed = tx.ErrConditionFailed
)

// Result represents a fluent query result.
type Result[T any] struct {
	data []T
	err  error
}

func (r *Result[T]) Filter(fn func(T) bool) *Result[T] {
	if r.err != nil {
		return r
	}
	filtered := make([]T, 0)
	for _, v := range r.data {
		if fn(v) {
			filtered = append(filtered, v)
		}
	}
	r.data = filtered
	return r
}

func (r *Result[T]) Sort(less func(i, j T) bool) *Result[T] {
	if r.err != nil {
		return r
	}
	sort.Slice(r.data, func(i, j int) bool {
		return less(r.data[i], r.data[j])
	})
	return r
}

func (r *Result[T]) Limit(n int) *Result[T] {
	if r.err != nil {
		return r
	}
	if n < len(r.data) {
		r.data = r.data[:n]
	}
	return r
}

func (r *Result[T]) Offset(n int) *Result[T] {
	if r.err != nil {
		return r
	}
	if n >= len(r.data) {
		r.data = nil
	} else {
		r.data = r.data[n:]
	}
	return r
}

func (r *Result[T]) All() ([]T, error) {
	return r.data, r.err
}

type Engine[T any] struct {
	mu         sync.RWMutex
	primary    index.Indexer
	secondary  *index.SecondaryIndex[T]
	wal        *wal.Manager
	codec      func(T) ([]byte, error)
	decoder    func([]byte) (T, error)
	merkle     *merkle.Tree
	bloom      *bloom.Filter
	encPool    *sync.Pool
	decPool    *sync.Pool
	valueCache cache.Cache[T]

	// Deduplication
	dedupEnabled bool
	dedup        map[uint64]int64
}

func New[T any](w *wal.Manager, codec func(T) ([]byte, error), decoder func([]byte) (T, error), opts ...Option[T]) *Engine[T] {
	e := &Engine[T]{
		primary:   index.NewMapIndex(),
		secondary: index.NewSecondaryIndex[T](),
		wal:       w,
		codec:     codec,
		decoder:   decoder,
		merkle:    merkle.New(),
		bloom:     bloom.New(1024*1024, 7),
		encPool: &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil)
				return enc
			},
		},
		decPool: &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil)
				return dec
			},
		},
		valueCache: cache.NewInMemory[T](cache.WithMaxEntries[T](100000)),
		dedup:      make(map[uint64]int64),
	}
	options.Apply(e, opts...)
	return e
}

func (e *Engine[T]) EnableDeduplication(v bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dedupEnabled = v
}

func (e *Engine[T]) Get(ctx context.Context, key string) (T, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if cached, ok, _ := e.valueCache.Get(ctx, key); ok {
		return cached, nil
	}

	if !e.bloom.MayContain(key) {
		var zero T
		return zero, ErrKeyNotFound
	}

	offset, ok := e.primary.Get(key)
	if !ok {
		var zero T
		return zero, ErrKeyNotFound
	}

	val, _, expiresAt, err := e.valueAt(offset, time.Now().UnixNano(), true)
	if err != nil {
		return val, err
	}
	var ttl time.Duration
	if expiresAt > 0 {
		ttl = time.Until(time.Unix(0, expiresAt))
		if ttl <= 0 {
			var zero T
			return zero, ErrKeyNotFound
		}
	}
	_ = e.valueCache.Set(ctx, key, val, ttl)
	return val, nil
}

func (e *Engine[T]) readAt(offset int64) (T, error) {
	value, _, _, err := e.valueAt(offset, time.Now().UnixNano(), true)
	return value, err
}

func (e *Engine[T]) GetByIndex(ctx context.Context, indexName string, value string) *Result[T] {
	e.mu.RLock()
	defer e.mu.RUnlock()
	pks := e.secondary.Get(indexName, value)

	// Collect primary offsets
	offsets := make([]int64, 0, len(pks))
	for _, pk := range pks {
		if offset, ok := e.primary.Get(pk); ok {
			offsets = append(offsets, offset)
		}
	}
	if len(offsets) == 0 {
		return &Result[T]{data: nil}
	}

	results := make([]T, 0, len(offsets))
	for _, offset := range offsets {
		if val, err := e.readAt(offset); err == nil {
			results = append(results, val)
		}
	}

	return &Result[T]{data: results}
}

func (e *Engine[T]) AddIndex(name string, extractor func(T) string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.secondary.AddIndex(name, extractor)
}

func (e *Engine[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	raw, compressed, err := e.encodeValue(value)
	if err != nil {
		return err
	}
	expiresAt := expiration(ttl, time.Now())

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.putPreparedLocked(ctx, key, value, raw, compressed, expiresAt)
}

func (e *Engine[T]) MerkleRoot() [32]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.merkle.Root()
}

func (e *Engine[T]) Bloom() *bloom.Filter {
	return e.bloom
}

func (e *Engine[T]) Delete(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if offset, ok := e.primary.Get(key); ok {
		if _, _, _, err := e.valueAt(offset, 0, false); err != nil {
			return err
		}
	}
	_, err := e.wal.Append(wal.Entry{Type: wal.EntryDelete, Key: key})
	if err != nil {
		return err
	}
	return e.applyDeleteLocked(ctx, key)
}

func (e *Engine[T]) Close() error {
	return e.wal.Close()
}

func (e *Engine[T]) Begin() (tx.Transaction[T], error) {
	return e.beginTransaction(), nil
}

func (e *Engine[T]) Keys() ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys []string
	now := time.Now().UnixNano()
	err := e.primary.ForEach(func(key string, offset int64) error {
		if _, _, _, err := e.valueAt(offset, now, true); err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return nil
			}
			return err
		}
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

func (e *Engine[T]) ForEach(fn func(key string, val T) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.primary.ForEach(func(key string, offset int64) error {
		val, err := e.readAt(offset)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return fn(key, val)
	})
}
