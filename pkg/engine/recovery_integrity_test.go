package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func encodedIntegrityEntry(t *testing.T, record integrityRecord) []byte {
	t.Helper()
	raw, err := integrityCodec(record)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	return encoder.EncodeAll(raw, nil)
}

func TestRecoverAppliesOnlyCommittedTransactions(t *testing.T) {
	dir := t.TempDir()
	manager, err := wal.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	base := encodedIntegrityEntry(t, integrityRecord{Value: "base", City: "Torino"})
	committed := encodedIntegrityEntry(t, integrityRecord{Value: "committed", City: "Milano"})
	uncommitted := encodedIntegrityEntry(t, integrityRecord{Value: "uncommitted", City: "Roma"})
	if _, err := manager.Append(wal.Entry{Type: wal.EntryPut, Key: "record", Value: base}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Append(wal.Entry{Type: wal.EntryPut, TxID: 10, Key: "record", Value: committed}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Append(wal.Entry{Type: wal.EntryCommit, TxID: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Append(wal.Entry{Type: wal.EntryPut, TxID: 11, Key: "record", Value: uncommitted}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Append(wal.Entry{Type: wal.EntryDelete, TxID: 12, Key: "record"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	engine, reopened := newIntegrityEngine(t, dir)
	defer reopened.Close()
	if err := engine.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := engine.Get(context.Background(), "record")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "committed" || got.City != "Milano" {
		t.Fatalf("recovered %#v", got)
	}
	old, _ := engine.GetByIndex(context.Background(), "city", "Torino").All()
	if len(old) != 0 {
		t.Fatalf("stale secondary index contains %d records", len(old))
	}
	current, _ := engine.GetByIndex(context.Background(), "city", "Milano").All()
	if len(current) != 1 {
		t.Fatalf("current secondary index contains %d records", len(current))
	}
}

func TestRecoverDiscardsRolledBackTransaction(t *testing.T) {
	dir := t.TempDir()
	manager, err := wal.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := encodedIntegrityEntry(t, integrityRecord{Value: "rolled-back"})
	if _, err := manager.Append(wal.Entry{Type: wal.EntryPut, TxID: 20, Key: "record", Value: value}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Append(wal.Entry{Type: wal.EntryRollback, TxID: 20}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	engine, reopened := newIntegrityEngine(t, dir)
	defer reopened.Close()
	if err := engine.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Get(context.Background(), "record"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("got %v, want missing key", err)
	}
}

func TestRecoverTransactionAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	engine, manager := newIntegrityEngine(t, dir)
	manager.SetMaxSegmentSize(90)
	transaction, _ := engine.Begin()
	ctx := context.Background()
	if err := transaction.Put(ctx, "first", integrityRecord{Value: "first-value"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put(ctx, "second", integrityRecord{Value: "second-value"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(manager.SealedSegments()) == 0 {
		t.Fatal("transaction did not span segments")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, reopened := newIntegrityEngine(t, dir)
	defer reopened.Close()
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"first", "second"} {
		if _, err := recovered.Get(ctx, key); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
	}
}

func TestRecoverExpiredWriteDoesNotResurrectPreviousValue(t *testing.T) {
	dir := t.TempDir()
	engine, manager := newIntegrityEngine(t, dir)
	ctx := context.Background()
	if err := engine.Put(ctx, "key", integrityRecord{Value: "old"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(ctx, "key", integrityRecord{Value: "new"}, 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	recovered, reopened := newIntegrityEngine(t, dir)
	defer reopened.Close()
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Get(ctx, "key"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("got %v, want expired key", err)
	}
}

func TestDeduplicatedKeysKeepIndependentTTL(t *testing.T) {
	dir := t.TempDir()
	engine, manager := newIntegrityEngine(t, dir, WithDeduplication[integrityRecord](true))
	ctx := context.Background()
	value := integrityRecord{Value: "same"}
	if err := engine.Put(ctx, "short", value, 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(ctx, "forever", value, 0); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	recovered, reopened := newIntegrityEngine(t, dir, WithDeduplication[integrityRecord](true))
	defer reopened.Close()
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Get(ctx, "short"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("short key: got %v", err)
	}
	if got, err := recovered.Get(ctx, "forever"); err != nil || got != value {
		t.Fatalf("forever key: got %#v, %v", got, err)
	}
}

func TestRecoverTruncatedActiveTailAndAppend(t *testing.T) {
	dir := t.TempDir()
	engine, manager := newIntegrityEngine(t, dir)
	ctx := context.Background()
	if err := engine.Put(ctx, "before", integrityRecord{Value: "before"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "0000000000000000.log")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	partial := wal.EncodeEntry(wal.Entry{Type: wal.EntryPut, Key: "torn", Value: []byte("torn")})
	if _, err := file.Write(partial[:len(partial)-3]); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, reopened := newIntegrityEngine(t, dir)
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Put(ctx, "after", integrityRecord{Value: "after"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	again, finalManager := newIntegrityEngine(t, dir)
	defer finalManager.Close()
	if err := again.Recover(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"before", "after"} {
		if _, err := again.Get(ctx, key); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
	}
}

func TestCompactionMaterializesDeduplicatedLinks(t *testing.T) {
	dir := t.TempDir()
	engine, manager := newIntegrityEngine(t, dir, WithDeduplication[integrityRecord](true))
	manager.SetMaxSegmentSize(80)
	ctx := context.Background()
	value := integrityRecord{Value: "shared-payload"}
	if err := engine.Put(ctx, "source", value, 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(ctx, "linked", value, 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(ctx, "source"); err != nil {
		t.Fatal(err)
	}
	if len(manager.SealedSegments()) == 0 {
		t.Fatal("test did not create a sealed segment")
	}
	if err := engine.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, reopened := newIntegrityEngine(t, dir, WithDeduplication[integrityRecord](true))
	defer reopened.Close()
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := recovered.Get(ctx, "linked"); err != nil || got != value {
		t.Fatalf("linked key: got %#v, %v", got, err)
	}
	if _, err := recovered.Get(ctx, "source"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("source key: got %v", err)
	}
}
