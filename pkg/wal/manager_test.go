package wal

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestManager_AppendRead(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer m.Close()

	key := "test-key"
	val := []byte("test-value")

	entry := Entry{Type: 0, Key: key, Value: val}
	offset, err := m.Append(entry)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	readVal, err := m.ReadAt(offset)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if !bytes.Equal(readVal, val) {
		t.Errorf("Read value mismatch. Got %s, want %s", readVal, val)
	}
}

func TestManager_Rotation(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	m.maxSize = 100
	defer m.Close()

	var offsets []int64
	for i := 0; i < 10; i++ {
		e := Entry{
			Type:  0,
			Key:   fmt.Sprintf("k%d", i),
			Value: []byte("value"),
		}
		off, err := m.Append(e)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
		offsets = append(offsets, off)
	}

	// Verify we have multiple segments
	entries, _ := os.ReadDir(dir)
	logCount := 0
	for _, e := range entries {
		if !e.IsDir() {
			logCount++
		}
	}

	if logCount < 2 {
		t.Errorf("Expected rotation, found %d log files", logCount)
	}

	// Read all back
	for i, off := range offsets {
		val, err := m.ReadAt(off)
		if err != nil {
			t.Fatalf("ReadAt %d failed: %v", i, err)
		}
		if string(val) != "value" {
			t.Errorf("Value mismatch at %d", i)
		}
	}
}

func TestManager_Reload(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	e := Entry{Type: 0, Key: "reload", Value: []byte("checked")}
	off, err := m.Append(e)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()

	// Re-open
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatalf("Failed to re-open manager: %v", err)
	}
	defer m2.Close()

	val, err := m2.ReadAt(off)
	if err != nil {
		t.Fatalf("ReadAt failed after reload: %v", err)
	}

	if string(val) != "checked" {
		t.Errorf("Read value mismatch")
	}

	if m2.active.ID() != 0 {
		t.Errorf("Expected active ID 0, got %d", m2.active.ID())
	}
}

func TestManager_OpenOnDemand(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	m.maxSize = 100
	defer m.Close()

	// Append something that will be in a sealed segment
	val1 := "value-that-is-exactly-60-bytes-long-0123456789012345678901234"
	e1 := Entry{Type: 0, Key: "k1", Value: []byte(val1)}
	off1, err := m.Append(e1)
	if err != nil {
		t.Fatal(err)
	}

	// Trigger rotation (e1+e2 > 100, but e2 < 100)
	e2 := Entry{Type: 0, Key: "k2", Value: []byte(val1)}
	if _, err := m.Append(e2); err != nil {
		t.Fatal(err)
	}

	// Verify k1 is in a sealed segment and its file is closed
	segID1, _ := UnpackOffset(off1)
	var sealedSeg *Segment
	for _, s := range m.sealed {
		if s.ID() == segID1 {
			sealedSeg = s
			break
		}
	}

	if sealedSeg == nil {
		t.Fatal("Sealed segment not found")
	}

	sealedSeg.mu.RLock()
	if sealedSeg.file != nil {
		sealedSeg.mu.RUnlock()
		t.Errorf("Sealed segment file should be closed")
	} else {
		sealedSeg.mu.RUnlock()
	}

	// Read k1 - should trigger open-on-demand
	read1, err := m.ReadAt(off1)
	if err != nil {
		t.Fatalf("ReadAt failed for closed segment: %v", err)
	}

	if string(read1) != val1 {
		t.Errorf("Value mismatch, got %s", read1)
	}

	// Verify file is now open
	sealedSeg.mu.RLock()
	if sealedSeg.file == nil {
		sealedSeg.mu.RUnlock()
		t.Errorf("Sealed segment file should be open after ReadAt")
	} else {
		sealedSeg.mu.RUnlock()
	}
}
