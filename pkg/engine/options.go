package engine

import (
	"github.com/mirkobrombin/go-foundation/pkg/options"
	"github.com/mirkobrombin/go-slipstream/pkg/bloom"
	"github.com/mirkobrombin/go-warp/v1/cache"
)

// Option defines a functional configuration for the Engine.
type Option[T any] = options.Option[Engine[T]]

// WithDeduplication enables or disables data deduplication.
func WithDeduplication[T any](enabled bool) Option[T] {
	return func(e *Engine[T]) {
		e.dedupEnabled = enabled
	}
}

// WithCacheSize sets the maximum number of entries in the value cache.
func WithCacheSize[T any](size int) Option[T] {
	return func(e *Engine[T]) {
		e.valueCache = cache.NewInMemory[T](cache.WithMaxEntries[T](size))
	}
}

// WithBloomFilter sets the size and hash count for the Bloom filter.
func WithBloomFilter[T any](size int, hashes int) Option[T] {
	return func(e *Engine[T]) {
		e.bloom = bloom.New(size, hashes)
	}
}
