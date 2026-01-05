package engine

import (
	"fmt"
	"time"

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
	sealed := e.wal.SealedSegments()
	if len(sealed) == 0 {
		return nil
	}

	for _, seg := range sealed {
		if err := e.compactSegment(seg); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine[T]) compactSegment(seg *wal.Segment) error {
	err := e.wal.IterateSegment(seg, func(entry wal.Entry, offset int64) error {
		if entry.Type != 0 {
			return nil
		}

		currentOff, ok := e.primary.Get(entry.Key)
		if !ok || currentOff != offset {
			return nil
		}

		newOffset, err := e.wal.Append(wal.Entry{
			Type:  0,
			Key:   entry.Key,
			Value: entry.Value,
		})
		if err != nil {
			return err
		}

		swapped := e.primary.CompareAndSwap(entry.Key, offset, newOffset)
		if !swapped {
			// If CompareAndSwap fails, it means a concurrent Put/Delete
			// has already updated the Primary Index, the entry is now stale (garbage),
			// and the user's update prevails, simply discard this compacted entry.
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Safe to remove the segment
	return e.wal.RemoveSegment(seg.ID())
}
