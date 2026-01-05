package sync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-warp/v1/syncbus"
)

// Manager handles the background synchronization between Slipstream nodes.
type Manager[T any] struct {
	engine   *engine.Engine[T]
	bus      syncbus.Bus
	interval time.Duration
}

// NewManager creates a new synchronization manager.
func NewManager[T any](e *engine.Engine[T], bus syncbus.Bus, interval time.Duration) *Manager[T] {
	return &Manager[T]{
		engine:   e,
		bus:      bus,
		interval: interval,
	}
}

// Start begins the background gossip and synchronization loop.
func (s *Manager[T]) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.GossipRoot(ctx)
		}
	}
}

// GossipRoot broadcasts the local Merkle root hash to the mesh.
func (s *Manager[T]) GossipRoot(ctx context.Context) {
	if s.bus == nil {
		return
	}

	root := s.engine.MerkleRoot()
	key := fmt.Sprintf("slipstream:root:%x", root)
	err := s.bus.Publish(ctx, key)
	if err != nil {
		slog.Error("slipstream: failed to gossip root", "error", err)
	}
}

// HandleRootEvent is called when a root hash is received from a peer.
func (s *Manager[T]) HandleRootEvent(ctx context.Context, peerRoot [32]byte) {
	localRoot := s.engine.MerkleRoot()
	if localRoot == peerRoot {
		return
	}

	slog.Info("slipstream: state divergence detected, initiating sync")
}
