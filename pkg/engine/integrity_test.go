package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

type integrityRecord struct {
	Value   string
	City    string
	Version int
}

func integrityCodec(record integrityRecord) ([]byte, error) {
	return []byte(fmt.Sprintf("%s|%s|%d", record.Value, record.City, record.Version)), nil
}

func integrityDecoder(data []byte) (integrityRecord, error) {
	parts := strings.Split(string(data), "|")
	if len(parts) != 3 {
		return integrityRecord{}, fmt.Errorf("invalid record")
	}
	version, err := strconv.Atoi(parts[2])
	if err != nil {
		return integrityRecord{}, err
	}
	return integrityRecord{Value: parts[0], City: parts[1], Version: version}, nil
}

func newIntegrityEngine(t *testing.T, dir string, opts ...Option[integrityRecord]) (*Engine[integrityRecord], *wal.Manager) {
	t.Helper()
	manager, err := wal.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine := New(manager, integrityCodec, integrityDecoder, opts...)
	engine.AddIndex("city", func(record integrityRecord) string { return record.City })
	return engine, manager
}

func TestPutIfAbsentConcurrentSingleWinner(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	start := make(chan struct{})
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(candidate int) {
			defer wg.Done()
			<-start
			err := engine.PutIfAbsent(ctx, "winner", integrityRecord{Value: fmt.Sprint(candidate)}, 0)
			if err == nil {
				successes.Add(1)
				return
			}
			if !errors.Is(err, ErrKeyExists) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("got %d successful inserts, want 1", successes.Load())
	}
}

func TestPutIfConcurrentExpectedVersionHasOneWinner(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	if err := engine.Put(ctx, "versioned", integrityRecord{Version: 1}, 0); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(candidate int) {
			defer wg.Done()
			<-start
			err := engine.PutIf(ctx, "versioned", integrityRecord{Version: candidate + 2}, 0, func(current integrityRecord, exists bool) bool {
				return exists && current.Version == 1
			})
			if err == nil {
				successes.Add(1)
				return
			}
			if !errors.Is(err, ErrConditionFailed) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("got %d successful writes, want 1", successes.Load())
	}
}

func TestConditionalTransactionsCommitAtomically(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	if err := engine.Put(ctx, "guard", integrityRecord{Version: 1}, 0); err != nil {
		t.Fatal(err)
	}

	transactions := make([]tx.ConditionalTransaction[integrityRecord], 2)
	for i := range transactions {
		transaction, err := engine.BeginConditional()
		if err != nil {
			t.Fatal(err)
		}
		if err := transaction.Require("guard", func(current integrityRecord, exists bool) bool {
			return exists && current.Version == 1
		}); err != nil {
			t.Fatal(err)
		}
		if err := transaction.Put(ctx, "guard", integrityRecord{Version: i + 2}, 0); err != nil {
			t.Fatal(err)
		}
		if err := transaction.Put(ctx, fmt.Sprintf("edge:%d", i), integrityRecord{Value: "edge"}, 0); err != nil {
			t.Fatal(err)
		}
		transactions[i] = transaction
	}

	start := make(chan struct{})
	results := make(chan error, len(transactions))
	for _, transaction := range transactions {
		go func(transaction tx.ConditionalTransaction[integrityRecord]) {
			<-start
			results <- transaction.Commit(ctx)
		}(transaction)
	}
	close(start)
	var successes int
	for range transactions {
		err := <-results
		if err == nil {
			successes++
		} else if !errors.Is(err, tx.ErrConditionFailed) {
			t.Fatalf("unexpected commit error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("got %d successful transactions, want 1", successes)
	}
	keys, err := engine.Keys()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want guard and one edge", len(keys))
	}
}

func TestFailedTransactionConditionWritesNothing(t *testing.T) {
	dir := t.TempDir()
	engine, manager := newIntegrityEngine(t, dir)
	ctx := context.Background()
	transaction, _ := engine.BeginConditional()
	if err := transaction.Require("missing", func(_ integrityRecord, exists bool) bool { return exists }); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put(ctx, "must-not-exist", integrityRecord{Value: "bad"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); !errors.Is(err, tx.ErrConditionFailed) {
		t.Fatalf("got %v, want condition failure", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, reopened := newIntegrityEngine(t, dir)
	defer reopened.Close()
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Get(ctx, "must-not-exist"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("got %v, want missing key", err)
	}
}

func TestDeleteRemovesSecondaryIndex(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	if err := engine.Put(ctx, "user", integrityRecord{Value: "user", City: "Torino"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(ctx, "user"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(ctx, "user", integrityRecord{Value: "user", City: "Milano"}, 0); err != nil {
		t.Fatal(err)
	}
	old, err := engine.GetByIndex(ctx, "city", "Torino").All()
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 0 {
		t.Fatalf("old index returned %d records", len(old))
	}
	current, _ := engine.GetByIndex(ctx, "city", "Milano").All()
	if len(current) != 1 {
		t.Fatalf("current index returned %d records, want 1", len(current))
	}
}

func TestTransactionDeleteRemovesSecondaryIndex(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	if err := engine.Put(ctx, "user", integrityRecord{Value: "user", City: "Torino"}, 0); err != nil {
		t.Fatal(err)
	}
	transaction, _ := engine.Begin()
	if err := transaction.Delete(ctx, "user"); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(ctx, "user", integrityRecord{Value: "user", City: "Milano"}, 0); err != nil {
		t.Fatal(err)
	}
	old, _ := engine.GetByIndex(ctx, "city", "Torino").All()
	if len(old) != 0 {
		t.Fatalf("old index returned %d records", len(old))
	}
}

func TestTransactionReadYourWritesAndLifecycle(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	transaction, _ := engine.Begin()
	want := integrityRecord{Value: "pending"}
	if err := transaction.Put(ctx, "key", want, 0); err != nil {
		t.Fatal(err)
	}
	got, err := transaction.Get(ctx, "key")
	if err != nil || got != want {
		t.Fatalf("got %#v, %v", got, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); !errors.Is(err, tx.ErrDone) {
		t.Fatalf("got %v, want completed transaction error", err)
	}
}

func TestCachedValueHonorsTTL(t *testing.T) {
	engine, manager := newIntegrityEngine(t, t.TempDir())
	defer manager.Close()
	ctx := context.Background()
	if err := engine.Put(ctx, "ttl", integrityRecord{Value: "short"}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Get(ctx, "ttl"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := engine.Get(ctx, "ttl"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("got %v, want expired key", err)
	}
}
