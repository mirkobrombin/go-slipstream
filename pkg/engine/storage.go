package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func (e *Engine[T]) encodeValue(value T) ([]byte, []byte, error) {
	raw, err := e.codec(value)
	if err != nil {
		return nil, nil, err
	}
	enc := e.encPool.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(raw, nil)
	e.encPool.Put(enc)
	return raw, compressed, nil
}

func (e *Engine[T]) compressedAt(offset int64) ([]byte, error) {
	seen := make(map[int64]struct{})
	for {
		if _, ok := seen[offset]; ok {
			return nil, fmt.Errorf("slipstream: cyclic WAL link at offset %d", offset)
		}
		seen[offset] = struct{}{}
		entry, err := e.wal.ReadEntryAt(offset)
		if err != nil {
			return nil, err
		}
		switch entry.Type {
		case wal.EntryPut:
			return entry.Value, nil
		case wal.EntryLink:
			if len(entry.Value) != 8 {
				return nil, fmt.Errorf("%w: link at offset %d has %d bytes", wal.ErrCorrupt, offset, len(entry.Value))
			}
			offset = int64(binary.BigEndian.Uint64(entry.Value))
		default:
			return nil, fmt.Errorf("%w: offset %d has entry type %d", wal.ErrCorrupt, offset, entry.Type)
		}
	}
}

func (e *Engine[T]) valueAt(offset int64, now int64, enforceTTL bool) (T, []byte, int64, error) {
	var zero T
	head, err := e.wal.ReadEntryAt(offset)
	if err != nil {
		return zero, nil, 0, err
	}
	if enforceTTL && head.ExpiresAt > 0 && now >= head.ExpiresAt {
		return zero, nil, head.ExpiresAt, ErrKeyNotFound
	}
	compressed, err := e.compressedAt(offset)
	if err != nil {
		return zero, nil, head.ExpiresAt, err
	}
	dec := e.decPool.Get().(*zstd.Decoder)
	raw, err := dec.DecodeAll(compressed, nil)
	e.decPool.Put(dec)
	if err != nil {
		return zero, nil, head.ExpiresAt, err
	}
	value, err := e.decoder(raw)
	if err != nil {
		return zero, nil, head.ExpiresAt, err
	}
	return value, raw, head.ExpiresAt, nil
}

func (e *Engine[T]) currentLocked(key string, now int64) (T, bool, error) {
	var zero T
	offset, ok := e.primary.Get(key)
	if !ok {
		return zero, false, nil
	}
	value, _, _, err := e.valueAt(offset, now, true)
	if errors.Is(err, ErrKeyNotFound) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func (e *Engine[T]) removeSecondaryLocked(key string) error {
	offset, ok := e.primary.Get(key)
	if !ok {
		return nil
	}
	value, _, _, err := e.valueAt(offset, 0, false)
	if err != nil {
		return err
	}
	for name, extractor := range e.secondary.Extractors() {
		e.secondary.RemoveEntry(name, extractor(value), key)
	}
	return nil
}

func (e *Engine[T]) applyPutLocked(ctx context.Context, key string, offset int64, value T, raw []byte) error {
	if err := e.removeSecondaryLocked(key); err != nil {
		return err
	}
	e.primary.Put(key, offset)
	e.merkle.Update(key, raw)
	e.bloom.Add(key)
	e.secondary.Update(key, value)
	_ = e.valueCache.Invalidate(ctx, key)
	return nil
}

func (e *Engine[T]) applyDeleteLocked(ctx context.Context, key string) error {
	if err := e.removeSecondaryLocked(key); err != nil {
		return err
	}
	e.primary.Delete(key)
	e.merkle.Delete(key)
	_ = e.valueCache.Invalidate(ctx, key)
	return nil
}

func (e *Engine[T]) dedupTargetLocked(compressed []byte, overlay map[uint64]int64) (int64, bool) {
	hash := xxhash.Sum64(compressed)
	if offset, ok := overlay[hash]; ok {
		stored, err := e.compressedAt(offset)
		if err == nil && bytes.Equal(stored, compressed) {
			return offset, true
		}
	}
	if offset, ok := e.dedup[hash]; ok {
		stored, err := e.compressedAt(offset)
		if err == nil && bytes.Equal(stored, compressed) {
			return offset, true
		}
	}
	return 0, false
}

func (e *Engine[T]) appendPutLocked(key string, compressed []byte, expiresAt int64, txID uint64, overlay map[uint64]int64) (wal.Entry, int64, error) {
	entry := wal.Entry{Type: wal.EntryPut, TxID: txID, Key: key, Value: compressed, ExpiresAt: expiresAt}
	if e.dedupEnabled {
		if target, ok := e.dedupTargetLocked(compressed, overlay); ok {
			link := make([]byte, 8)
			binary.BigEndian.PutUint64(link, uint64(target))
			entry.Type = wal.EntryLink
			entry.Value = link
		}
	}
	offset, err := e.wal.Append(entry)
	if err != nil {
		return wal.Entry{}, 0, err
	}
	if e.dedupEnabled && entry.Type == wal.EntryPut {
		overlay[xxhash.Sum64(compressed)] = offset
	}
	return entry, offset, nil
}

func (e *Engine[T]) putPreparedLocked(ctx context.Context, key string, value T, raw, compressed []byte, expiresAt int64) error {
	if offset, ok := e.primary.Get(key); ok {
		if _, _, _, err := e.valueAt(offset, 0, false); err != nil {
			return err
		}
	}
	entry, offset, err := e.appendPutLocked(key, compressed, expiresAt, 0, make(map[uint64]int64))
	if err != nil {
		return err
	}
	if err := e.applyPutLocked(ctx, key, offset, value, raw); err != nil {
		return err
	}
	if e.dedupEnabled && entry.Type == wal.EntryPut {
		e.dedup[xxhash.Sum64(compressed)] = offset
	}
	return nil
}

func expiration(ttl time.Duration, now time.Time) int64 {
	if ttl <= 0 {
		return 0
	}
	return now.Add(ttl).UnixNano()
}
