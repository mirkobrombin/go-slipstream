package tx

import (
	"context"
	"time"
)

// Transaction defines the interface for atomic operations.
type Transaction[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Put(ctx context.Context, key string, value T, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Commit(ctx context.Context) error
	Rollback() error
}

// Op represents a buffered operation in a transaction.
type Op struct {
	Type  byte
	Key   string
	Value []byte
}
