package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/mirkobrombin/go-foundation/pkg/options"
	"github.com/mirkobrombin/go-slipstream/pkg/bloom"
	"github.com/mirkobrombin/go-slipstream/pkg/index"
	"github.com/mirkobrombin/go-slipstream/pkg/merkle"
	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
	"github.com/mirkobrombin/go-warp/v1/cache"
)

var (
	ErrClosed      = fmt.Errorf("slipstream: storage is closed")
	ErrKeyNotFound = fmt.Errorf("slipstream: key not found")
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
	if cached, ok, _ := e.valueCache.Get(ctx, key); ok {
		return cached, nil
	}

	e.mu.RLock()
	// Bloom check before anything else
	if !e.bloom.MayContain(key) {
		e.mu.RUnlock()
		var zero T
		return zero, fmt.Errorf("engine: not found")
	}

	offset, ok := e.primary.Get(key)
	e.mu.RUnlock() // Release lock for I/O and decompression

	if !ok {
		var zero T
		return zero, fmt.Errorf("engine: not found")
	}

	val, err := e.readAt(offset)
	if err != nil {
		return val, err
	}
	_ = e.valueCache.Set(ctx, key, val, 0)
	return val, nil
}

func (e *Engine[T]) readAt(offset int64) (T, error) {
	var zero T
	entry, err := e.wal.ReadEntryAt(offset)
	if err != nil {
		return zero, err
	}

	if entry.Type == wal.EntryLink {
		targetOffset := int64(binary.BigEndian.Uint64(entry.Value))
		return e.readAt(targetOffset)
	}

	dec := e.decPool.Get().(*zstd.Decoder)
	defer e.decPool.Put(dec)

	decompressed, err := dec.DecodeAll(entry.Value, nil)
	if err != nil {
		return zero, err
	}
	return e.decoder(decompressed)
}

func (e *Engine[T]) GetByIndex(ctx context.Context, indexName string, value string) *Result[T] {
	e.mu.RLock()
	// Get offsets first to minimize lock time
	pks := e.secondary.Get(indexName, value)

	// Collect primary offsets
	offsets := make([]int64, 0, len(pks))
	for _, pk := range pks {
		if offset, ok := e.primary.Get(pk); ok {
			offsets = append(offsets, offset)
		}
	}
	e.mu.RUnlock()

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
	e.secondary.AddIndex(name, extractor)
}

func (e *Engine[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	data, err := e.codec(value)
	if err != nil {
		return err
	}

	enc := e.encPool.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(data, nil)
	e.encPool.Put(enc)

	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if oldOffset, ok := e.primary.Get(key); ok {
		if oldVal, err := e.readAt(oldOffset); err == nil {
			for name, extractor := range e.secondary.Extractors() {
				e.secondary.RemoveEntry(name, extractor(oldVal), key)
			}
		}
	}

	var offset int64
	var walEntry wal.Entry
	walEntry.Key = key
	walEntry.ExpiresAt = expiresAt

	if e.dedupEnabled {
		h := xxhash.Sum64(compressed)
		if existingOffset, ok := e.dedup[h]; ok {
			linkBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(linkBuf, uint64(existingOffset))
			walEntry.Type = wal.EntryLink
			walEntry.Value = linkBuf
		} else {
			walEntry.Type = wal.EntryPut
			walEntry.Value = compressed
		}
	} else {
		walEntry.Type = wal.EntryPut
		walEntry.Value = compressed
	}

	offset, err = e.wal.Append(walEntry)
	if err != nil {
		return err
	}

	if e.dedupEnabled && walEntry.Type == wal.EntryPut {
		h := xxhash.Sum64(compressed)
		e.dedup[h] = offset
	}

	e.primary.Put(key, offset)
	e.merkle.Update(key, data) // Merkle uses original data
	e.bloom.Add(key)
	e.secondary.Update(key, value)

	_ = e.valueCache.Invalidate(ctx, key)
	return nil
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

	_, err := e.wal.Append(wal.Entry{Type: 1, Key: key})
	if err != nil {
		return err
	}

	e.primary.Delete(key)
	e.merkle.Delete(key)
	_ = e.valueCache.Invalidate(ctx, key)
	return nil
}

func (e *Engine[T]) Close() error {
	return e.wal.Close()
}

func (e *Engine[T]) Begin() (tx.Transaction[T], error) {
	return &btx[T]{
		engine: e,
		txID:   e.wal.NextTxID(),
	}, nil
}

type btx[T any] struct {
	engine *Engine[T]
	txID   uint64
	ops    []wal.Entry
}

func (b *btx[T]) Get(ctx context.Context, key string) (T, error) {
	return b.engine.Get(ctx, key)
}

func (b *btx[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	data, _ := b.engine.codec(value)

	enc := b.engine.encPool.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(data, nil)
	b.engine.encPool.Put(enc)

	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	b.ops = append(b.ops, wal.Entry{
		Type:      0,
		TxID:      b.txID,
		Key:       key,
		Value:     compressed,
		ExpiresAt: expiresAt,
	})
	return nil
}

func (b *btx[T]) Delete(ctx context.Context, key string) error {
	b.ops = append(b.ops, wal.Entry{Type: 1, TxID: b.txID, Key: key})
	return nil
}

func (b *btx[T]) Commit(ctx context.Context) error {
	b.engine.mu.Lock()
	defer b.engine.mu.Unlock()

	offsets := make([]int64, len(b.ops))
	for i, op := range b.ops {
		finalOp := op
		if b.engine.dedupEnabled && op.Type == wal.EntryPut {
			h := xxhash.Sum64(op.Value)
			if existingOffset, ok := b.engine.dedup[h]; ok {
				linkBuf := make([]byte, 8)
				binary.BigEndian.PutUint64(linkBuf, uint64(existingOffset))
				finalOp.Type = wal.EntryLink
				finalOp.Value = linkBuf
			}
		}

		off, err := b.engine.wal.Append(finalOp)
		if err != nil {
			return err
		}
		offsets[i] = off

		if b.engine.dedupEnabled && finalOp.Type == wal.EntryPut {
			h := xxhash.Sum64(finalOp.Value)
			b.engine.dedup[h] = off
		}
	}

	_, err := b.engine.wal.Append(wal.Entry{Type: 2, TxID: b.txID})
	if err != nil {
		return err
	}

	for i, op := range b.ops {
		if op.Type == 0 {
			if oldOff, ok := b.engine.primary.Get(op.Key); ok {
				if oldVal, err := b.engine.readAt(oldOff); err == nil {
					for name, ext := range b.engine.secondary.Extractors() {
						b.engine.secondary.RemoveEntry(name, ext(oldVal), op.Key)
					}
				}
			}
			b.engine.primary.Put(op.Key, offsets[i])
			b.engine.merkle.Update(op.Key, op.Value)
			b.engine.bloom.Add(op.Key)

			dec := b.engine.decPool.Get().(*zstd.Decoder)
			decomp, _ := dec.DecodeAll(op.Value, nil)
			b.engine.decPool.Put(dec)

			val, _ := b.engine.decoder(decomp)
			b.engine.secondary.Update(op.Key, val)
		} else {
			b.engine.primary.Delete(op.Key)
			b.engine.merkle.Delete(op.Key)
		}
	}
	return b.engine.wal.Sync()
}

func (e *Engine[T]) Keys() ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys []string
	err := e.primary.ForEach(func(key string, _ int64) error {
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
			return err
		}
		return fn(key, val)
	})
}

func (b *btx[T]) Rollback() error {
	b.ops = nil
	return nil
}

// Recover rebuilds the index from the WAL.
func (e *Engine[T]) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	processEntry := func(entry wal.Entry, offset int64) error {
		if entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
			return nil
		}

		switch entry.Type {
		case wal.EntryPut:
			e.primary.Put(entry.Key, offset)
			e.bloom.Add(entry.Key)
			if e.dedupEnabled {
				h := xxhash.Sum64(entry.Value)
				e.dedup[h] = offset
			}
		case wal.EntryLink:
			e.primary.Put(entry.Key, offset)
		case wal.EntryDelete:
			e.primary.Delete(entry.Key)
			e.merkle.Delete(entry.Key)
		}
		return nil
	}

	// 1. Replay Sealed Segments
	sealed := e.wal.SealedSegments()
	for _, seg := range sealed {
		if err := e.wal.IterateSegment(seg, processEntry); err != nil {
			return err
		}
	}

	return e.wal.IterateActiveSegment(processEntry)
}
