package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/mirkobrombin/go-slipstream/pkg/merkle"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

type recoveredOp struct {
	entry  wal.Entry
	offset int64
}

// Recover rebuilds in-memory state and applies transaction operations only
// when their commit marker is present.
func (e *Engine[T]) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.resetRecoveredStateLocked(); err != nil {
		return err
	}
	pending := make(map[uint64][]recoveredOp)
	allowedValues := make(map[int64]struct{})
	recoveryTime := time.Now().UnixNano()

	apply := func(op recoveredOp) error {
		entry := op.entry
		switch entry.Type {
		case wal.EntryPut:
			allowedValues[op.offset] = struct{}{}
			if e.dedupEnabled {
				e.dedup[xxhash.Sum64(entry.Value)] = op.offset
			}
		case wal.EntryLink:
			if len(entry.Value) != 8 {
				return fmt.Errorf("%w: link at offset %d has %d bytes", wal.ErrCorrupt, op.offset, len(entry.Value))
			}
			target := int64(binary.BigEndian.Uint64(entry.Value))
			if _, ok := allowedValues[target]; !ok {
				return fmt.Errorf("%w: link at offset %d targets unavailable offset %d", wal.ErrCorrupt, op.offset, target)
			}
			allowedValues[op.offset] = struct{}{}
		case wal.EntryDelete:
			return e.applyDeleteLocked(context.Background(), entry.Key)
		default:
			return fmt.Errorf("%w: invalid data entry type %d", wal.ErrCorrupt, entry.Type)
		}

		if entry.ExpiresAt > 0 && recoveryTime >= entry.ExpiresAt {
			return e.applyDeleteLocked(context.Background(), entry.Key)
		}
		value, raw, _, err := e.valueAt(op.offset, recoveryTime, false)
		if err != nil {
			return err
		}
		return e.applyPutLocked(context.Background(), entry.Key, op.offset, value, raw)
	}

	process := func(entry wal.Entry, offset int64) error {
		switch entry.Type {
		case wal.EntryPut, wal.EntryDelete, wal.EntryLink:
			op := recoveredOp{entry: entry, offset: offset}
			if entry.TxID == 0 {
				return apply(op)
			}
			pending[entry.TxID] = append(pending[entry.TxID], op)
			return nil
		case wal.EntryCommit:
			if entry.TxID == 0 {
				return fmt.Errorf("%w: commit marker has no transaction ID", wal.ErrCorrupt)
			}
			ops := pending[entry.TxID]
			for _, op := range ops {
				if op.entry.Type == wal.EntryPut || op.entry.Type == wal.EntryLink {
					allowedValues[op.offset] = struct{}{}
				}
			}
			for _, op := range ops {
				if err := apply(op); err != nil {
					return err
				}
			}
			delete(pending, entry.TxID)
			return nil
		case wal.EntryRollback:
			if entry.TxID == 0 {
				return fmt.Errorf("%w: rollback marker has no transaction ID", wal.ErrCorrupt)
			}
			delete(pending, entry.TxID)
			return nil
		default:
			return fmt.Errorf("%w: unknown entry type %d at offset %d", wal.ErrCorrupt, entry.Type, offset)
		}
	}

	for _, segment := range e.wal.SealedSegments() {
		if err := e.wal.IterateSegment(segment, process); err != nil {
			return err
		}
	}
	return e.wal.IterateActiveSegment(process)
}

func (e *Engine[T]) resetRecoveredStateLocked() error {
	keys := make([]string, 0)
	if err := e.primary.ForEach(func(key string, _ int64) error {
		keys = append(keys, key)
		return nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		e.primary.Delete(key)
		_ = e.valueCache.Invalidate(context.Background(), key)
	}
	e.secondary.Reset()
	e.bloom.Reset()
	e.merkle = merkle.New()
	e.dedup = make(map[uint64]int64)
	return nil
}
