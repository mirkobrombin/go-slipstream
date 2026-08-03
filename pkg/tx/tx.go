package tx

import (
	"context"
	"errors"
	"time"
)

var (
	ErrConditionFailed  = errors.New("transaction condition failed")
	ErrInvalidCondition = errors.New("transaction condition is nil")
	ErrDone             = errors.New("transaction already completed")
	ErrCommitUncertain  = errors.New("transaction commit outcome is uncertain")
)

// Condition evaluates the committed value for a key while the engine write
// lock is held. Missing and expired keys are reported with exists set to false.
// A condition must not call back into the same engine.
type Condition[T any] func(current T, exists bool) bool

// Transaction defines the interface for atomic operations.
type Transaction[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Put(ctx context.Context, key string, value T, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Commit(ctx context.Context) error
	Rollback() error
}

// ConditionalTransaction adds commit-time checks without changing the base
// Transaction interface implemented by existing adapters.
type ConditionalTransaction[T any] interface {
	Transaction[T]
	Require(key string, condition Condition[T]) error
}

// Op represents a buffered operation in a transaction.
type Op struct {
	Type  byte
	Key   string
	Value []byte
}
