package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDirectoryLockAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	runDirectoryLockHelper(t, dir, "locked")
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	runDirectoryLockHelper(t, dir, "open")
}

func TestDirectoryLockHelper(t *testing.T) {
	dir := os.Getenv("SLIPSTREAM_LOCK_TEST_DIR")
	if dir == "" {
		return
	}
	expected := os.Getenv("SLIPSTREAM_LOCK_TEST_EXPECTED")
	manager, err := NewManager(dir)
	if expected == "locked" {
		if !errors.Is(err, ErrDirectoryLocked) {
			t.Fatalf("got %v, want directory lock error", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func runDirectoryLockHelper(t *testing.T, dir, expected string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestDirectoryLockHelper$")
	command.Env = append(os.Environ(),
		"SLIPSTREAM_LOCK_TEST_DIR="+dir,
		"SLIPSTREAM_LOCK_TEST_EXPECTED="+expected,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("helper failed: %v\n%s", err, output)
	}
}

func TestSealedSegmentTruncationIsCorruption(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetMaxSegmentSize(80)
	entry := Entry{Type: EntryPut, Key: "key", Value: []byte("01234567890123456789")}
	if _, err := manager.Append(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Append(entry); err != nil {
		t.Fatal(err)
	}
	sealed := manager.SealedSegments()
	if len(sealed) != 1 {
		t.Fatalf("got %d sealed segments, want 1", len(sealed))
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "0000000000000000.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}

	_, err = NewManager(dir)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want corruption error", err)
	}
}

func TestChecksummedEntryRejectsPayloadBitFlip(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{Type: EntryPut, Key: "key", Value: []byte("payload")}
	if _, err := manager.Append(entry); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "0000000000000000.log")
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	valueOffset := int64(checksummedHeaderSize + len(entry.Key) + 4)
	byteAtOffset := []byte{0}
	if _, err := file.ReadAt(byteAtOffset, valueOffset); err != nil {
		t.Fatal(err)
	}
	byteAtOffset[0] ^= 0xff
	if _, err := file.WriteAt(byteAtOffset, valueOffset); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want checksum corruption", err)
	}
}

func TestActiveHeaderCorruptionIsNotTruncated(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := manager.Append(Entry{Type: EntryPut, Key: string(rune('a' + i)), Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "0000000000000000.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	firstSize := int64(len(EncodeEntry(Entry{Type: EntryPut, Key: "a", Value: []byte("value")})))
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	lengthByte := []byte{0}
	if _, err := file.ReadAt(lengthByte, firstSize+1); err != nil {
		t.Fatal(err)
	}
	lengthByte[0] ^= 0x01
	if _, err := file.WriteAt(lengthByte, firstSize+1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want header corruption", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != info.Size() {
		t.Fatalf("corrupt active segment was truncated from %d to %d", info.Size(), after.Size())
	}
}

func TestTransactionIDRestoredFromWAL(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	previous := uint64(time.Now().UnixNano()) + 1<<32
	if _, err := manager.Append(Entry{Type: EntryPut, TxID: previous, Key: "pending", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.NextTxID(); got != previous+1 {
		t.Fatalf("got transaction ID %d, want %d", got, previous+1)
	}
}

func TestLegacyEntryCompatibility(t *testing.T) {
	dir := t.TempDir()
	entry := Entry{Type: EntryPut, TxID: 42, Key: "legacy", Value: []byte("value")}
	path := filepath.Join(dir, "0000000000000000.log")
	if err := os.WriteFile(path, encodeLegacyEntry(entry), 0600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	read, err := manager.ReadEntryAt(PackOffset(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if read.Type != entry.Type || read.TxID != entry.TxID || read.Key != entry.Key || string(read.Value) != string(entry.Value) {
		t.Fatalf("decoded %#v, want %#v", read, entry)
	}
}

func encodeLegacyEntry(entry Entry) []byte {
	buf := make([]byte, 25+len(entry.Key)+len(entry.Value))
	buf[0] = entry.Type
	binary.BigEndian.PutUint64(buf[1:], entry.TxID)
	binary.BigEndian.PutUint64(buf[9:], uint64(entry.ExpiresAt))
	binary.BigEndian.PutUint32(buf[17:], uint32(len(entry.Key)))
	copy(buf[21:], entry.Key)
	valueLengthOffset := 21 + len(entry.Key)
	binary.BigEndian.PutUint32(buf[valueLengthOffset:], uint32(len(entry.Value)))
	copy(buf[valueLengthOffset+4:], entry.Value)
	return buf
}
