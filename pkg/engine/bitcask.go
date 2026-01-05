package engine

import (
	"context"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

// Bitcask is a wrapper around the modular engine.
type Bitcask[T any] struct {
	engine *Engine[T]
}

// NewBitcask creates a new Bitcask engine.
func NewBitcask[T any](w *wal.Manager, codec func(T) ([]byte, error), decoder func([]byte) (T, error)) *Bitcask[T] {
	e := New[T](w, codec, decoder)
	return &Bitcask[T]{engine: e}
}

func (b *Bitcask[T]) Engine() *Engine[T] {
	return b.engine
}

func (b *Bitcask[T]) AddIndex(name string, extractor func(T) string) {
	b.engine.AddIndex(name, extractor)
}

func (b *Bitcask[T]) Get(ctx context.Context, key string) (T, error) {
	return b.engine.Get(ctx, key)
}

func (b *Bitcask[T]) GetByIndex(ctx context.Context, indexName string, value string) *Result[T] {
	return b.engine.GetByIndex(ctx, indexName, value)
}

func (b *Bitcask[T]) Put(ctx context.Context, key string, value T, ttl time.Duration) error {
	return b.engine.Put(ctx, key, value, ttl)
}

func (b *Bitcask[T]) Delete(ctx context.Context, key string) error {
	return b.engine.Delete(ctx, key)
}

func (b *Bitcask[T]) MerkleRoot() [32]byte {
	return b.engine.MerkleRoot()
}

func (b *Bitcask[T]) Close() error {
	return b.engine.Close()
}

func (b *Bitcask[T]) Begin() (tx.Transaction[T], error) {
	return b.engine.Begin()
}

func (b *Bitcask[T]) Compact() error {
	return b.engine.Compact()
}
