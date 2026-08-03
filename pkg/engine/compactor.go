package engine

import (
	"fmt"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

// StartCompactor starts the background compaction routine.
func (e *Engine[T]) StartCompactor(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := e.Compact(); err != nil {
				fmt.Printf("slipstream: compaction error: %v\n", err)
			}
		}
	}()
}

func (e *Engine[T]) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.wal.SealedSegments()) == 0 {
		return nil
	}

	if err := e.wal.Rotate(); err != nil {
		return err
	}
	sources := e.wal.SealedSegments()
	type currentEntry struct {
		key       string
		offset    int64
		expiresAt int64
		value     []byte
	}
	current := make([]currentEntry, 0)
	if err := e.primary.ForEach(func(key string, offset int64) error {
		head, err := e.wal.ReadEntryAt(offset)
		if err != nil {
			return err
		}
		compressed, err := e.compressedAt(offset)
		if err != nil {
			return err
		}
		current = append(current, currentEntry{
			key:       key,
			offset:    offset,
			expiresAt: head.ExpiresAt,
			value:     compressed,
		})
		return nil
	}); err != nil {
		return err
	}

	for _, entry := range current {
		newOffset, err := e.wal.Append(wal.Entry{
			Type:      wal.EntryPut,
			Key:       entry.key,
			Value:     entry.value,
			ExpiresAt: entry.expiresAt,
		})
		if err != nil {
			return err
		}
		e.primary.Put(entry.key, newOffset)
	}
	if err := e.wal.Sync(); err != nil {
		return err
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].ID() > sources[j].ID() })
	for _, segment := range sources {
		if err := e.wal.RemoveSegment(segment.ID()); err != nil {
			return err
		}
	}
	return e.rebuildDedupLocked()
}

func (e *Engine[T]) rebuildDedupLocked() error {
	e.dedup = make(map[uint64]int64)
	if !e.dedupEnabled {
		return nil
	}
	return e.primary.ForEach(func(_ string, offset int64) error {
		compressed, err := e.compressedAt(offset)
		if err != nil {
			return err
		}
		e.dedup[xxhash.Sum64(compressed)] = offset
		return nil
	})
}
