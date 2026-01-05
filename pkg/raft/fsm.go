package raft

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mirkobrombin/go-slipstream/pkg/engine"
)

type OpType string

const (
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
)

type command struct {
	Op    OpType        `json:"op"`
	Key   string        `json:"key"`
	Value []byte        `json:"value"`
	TTL   time.Duration `json:"ttl"`
}

type fsm[T any] struct {
	engine *engine.Engine[T]
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)
}

func (f *fsm[T]) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		slog.Error("raft fsm: failed to unmarshal command", "err", err)
		return err
	}

	ctx := context.Background()

	switch cmd.Op {
	case OpPut:
		val, err := f.decode(cmd.Value)
		if err != nil {
			slog.Error("raft fsm: failed to decode value", "err", err)
			return err
		}
		if err := f.engine.Put(ctx, cmd.Key, val, cmd.TTL); err != nil {
			slog.Error("raft fsm: put failed", "key", cmd.Key, "err", err)
			return err
		}
		return nil

	case OpDelete:
		if err := f.engine.Delete(ctx, cmd.Key); err != nil {
			slog.Error("raft fsm: delete failed", "key", cmd.Key, "err", err)
			return err
		}
		return nil

	default:
		return nil
	}
}

func (f *fsm[T]) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot[T]{
		engine: f.engine,
		encode: f.encode,
	}, nil
}

func (f *fsm[T]) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	decoder := json.NewDecoder(rc)
	for {
		var cmd command
		if err := decoder.Decode(&cmd); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		ctx := context.Background()
		if cmd.Op == OpPut {
			val, err := f.decode(cmd.Value)
			if err != nil {
				return err
			}
			if err := f.engine.Put(ctx, cmd.Key, val, cmd.TTL); err != nil {
				return err
			}
		}
	}
	return nil
}

type snapshot[T any] struct {
	engine *engine.Engine[T]
	encode func(T) ([]byte, error)
}

func (s *snapshot[T]) Persist(sink raft.SnapshotSink) error {
	encoder := json.NewEncoder(sink)
	err := s.engine.ForEach(func(key string, val T) error {
		data, err := s.encode(val)
		if err != nil {
			return err
		}
		cmd := command{
			Op:    OpPut,
			Key:   key,
			Value: data,
		}
		return encoder.Encode(cmd)
	})

	if err != nil {
		sink.Cancel()
		return err
	}

	sink.Close()
	return nil
}

func (s *snapshot[T]) Release() {}
