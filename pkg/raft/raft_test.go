package raft

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func TestRaft_SingleNode(t *testing.T) {
	dir := t.TempDir()

	// Data dirs
	engineDir := fmt.Sprintf("%s/engine", dir)
	raftDir := fmt.Sprintf("%s/raft", dir)
	_ = os.MkdirAll(engineDir, 0755)
	_ = os.MkdirAll(raftDir, 0755)

	// Engine
	w, _ := wal.NewManager(engineDir)
	codec := func(s string) ([]byte, error) { return []byte(s), nil }
	decoder := func(b []byte) (string, error) { return string(b), nil }
	e := engine.New[string](w, codec, decoder)
	defer e.Close()

	// Raft
	cfg := &Config{
		NodeID:    "node1",
		BindAddr:  "127.0.0.1:40001",
		DataDir:   raftDir,
		Bootstrap: true,
	}

	rm, err := NewManager(e, codec, decoder, cfg)
	if err != nil {
		t.Fatalf("failed to create raft manager: %v", err)
	}
	defer rm.Close()

	// Wait for leader
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

WaitLoop:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for leader")
		case <-ticker.C:
			if rm.Leader() {
				break WaitLoop
			}
		}
	}

	// Propose
	err = rm.Propose(context.Background(), "raft-key", "raft-value", 0)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Verify in engine
	// Note: Apply is async usually, but rm.Propose waits for future.Error()
	val, err := e.Get(context.Background(), "raft-key")
	if err != nil {
		t.Fatalf("get from engine failed: %v", err)
	}
	if val != "raft-value" {
		t.Errorf("expected raft-value, got %s", val)
	}
}
