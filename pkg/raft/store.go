package raft

import (
	"context"
	"encoding/binary"
	"encoding/gob"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

// SlipstreamStore implements raft.LogStore and raft.StableStore
type SlipstreamStore struct {
	engine *engine.Engine[[]byte]
}

// NewSlipstreamStore creates a new Raft store backed by Slipstream
func NewSlipstreamStore(path string) (*SlipstreamStore, error) {
	manager, err := wal.NewManager(path)
	if err != nil {
		return nil, err
	}

	codec := func(b []byte) ([]byte, error) { return b, nil }
	decoder := func(b []byte) ([]byte, error) { return b, nil }

	// Using small segments for Raft logs as they are frequently compacted
	manager.SetMaxSegmentSize(10 * 1024 * 1024)

	eng := engine.New[[]byte](manager, codec, decoder)
	if err := eng.Recover(); err != nil {
		return nil, err
	}

	return &SlipstreamStore{
		engine: eng,
	}, nil
}

func (s *SlipstreamStore) Close() error {
	return s.engine.Close()
}

func (s *SlipstreamStore) Set(key []byte, val []byte) error {
	return s.engine.Put(context.Background(), "meta:"+string(key), val, 0)
}

func (s *SlipstreamStore) Get(key []byte) ([]byte, error) {
	val, err := s.engine.Get(context.Background(), "meta:"+string(key))
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *SlipstreamStore) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return s.Set(key, b)
}

func (s *SlipstreamStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil // Default to 0
	}
	return binary.BigEndian.Uint64(val), nil
}

func logKey(currIndex uint64) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, currIndex)
	return "log:" + string(b)
}

func (s *SlipstreamStore) FirstIndex() (uint64, error) {
	return s.GetUint64([]byte("first_index"))
}

func (s *SlipstreamStore) LastIndex() (uint64, error) {
	return s.GetUint64([]byte("last_index"))
}

func (s *SlipstreamStore) GetLog(index uint64, log *raft.Log) error {
	val, err := s.engine.Get(context.Background(), logKey(index))
	if err != nil {
		return raft.ErrLogNotFound
	}

	return gob.NewDecoder(strings.NewReader(string(val))).Decode(log)
}

func (s *SlipstreamStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *SlipstreamStore) StoreLogs(logs []*raft.Log) error {
	ctx := context.Background()

	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()

	if len(logs) == 0 {
		return nil
	}

	newFirst := first
	newLast := last
	if first == 0 {
		newFirst = logs[0].Index
	}

	tx, _ := s.engine.Begin()

	for _, l := range logs {
		var buf strings.Builder
		if err := gob.NewEncoder(&buf).Encode(l); err != nil {
			return err
		}

		key := logKey(l.Index)
		tx.Put(ctx, key, []byte(buf.String()), 0)

		if l.Index > newLast {
			newLast = l.Index
		}
		if l.Index < newFirst {
			newFirst = l.Index
		}
	}

	// Update metadata
	bFirst := make([]byte, 8)
	binary.BigEndian.PutUint64(bFirst, newFirst)
	tx.Put(ctx, "meta:first_index", bFirst, 0)

	bLast := make([]byte, 8)
	binary.BigEndian.PutUint64(bLast, newLast)
	tx.Put(ctx, "meta:last_index", bLast, 0)

	return tx.Commit(ctx)
}

func (s *SlipstreamStore) DeleteRange(min, max uint64) error {
	ctx := context.Background()
	tx, _ := s.engine.Begin()

	for i := min; i <= max; i++ {
		tx.Delete(ctx, logKey(i))
	}

	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()

	if min <= first {
		newFirst := max + 1
		bFirst := make([]byte, 8)
		binary.BigEndian.PutUint64(bFirst, newFirst)
		tx.Put(ctx, "meta:first_index", bFirst, 0)
	}

	if max >= last {
		newLast := min - 1
		bLast := make([]byte, 8)
		binary.BigEndian.PutUint64(bLast, newLast)
		tx.Put(ctx, "meta:last_index", bLast, 0)
	}

	return tx.Commit(ctx)
}
