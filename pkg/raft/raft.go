package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mirkobrombin/go-slipstream/pkg/engine"
)

// Config holds Raft cluster configuration.
type Config struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Bootstrap bool
	Peers     []string
}

// Manager handles the strong consistency operations using Raft.
type Manager[T any] struct {
	engine *engine.Engine[T]
	raft   *raft.Raft
	config *Config
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)
}

// NewManager creates a new Raft manager and starts the node.
func NewManager[T any](e *engine.Engine[T], enc func(T) ([]byte, error), dec func([]byte) (T, error), cfg *Config) (*Manager[T], error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)

	// FSM
	fsm := &fsm[T]{
		engine: e,
		encode: enc,
		decode: dec,
	}

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Snapshots
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Stable and Log storage using Slipstream
	// We use a separate sub-directory for Raft internals
	raftStoreDir := filepath.Join(cfg.DataDir, "store")
	store, err := NewSlipstreamStore(raftStoreDir)
	if err != nil {
		return nil, err
	}

	// Instantiate Raft
	r, err := raft.NewRaft(raftCfg, fsm, store, store, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// Bootstrap if required
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftCfg.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return &Manager[T]{
		engine: e,
		raft:   r,
		config: cfg,
		encode: enc,
		decode: dec,
	}, nil
}

// Propose appends a new state change via the Raft cluster.
func (m *Manager[T]) Propose(ctx context.Context, key string, value T, ttl time.Duration) error {
	data, err := m.encode(value)
	if err != nil {
		return err
	}
	return m.ProposeRaw(ctx, OpPut, key, data, ttl)
}

func (m *Manager[T]) Delete(ctx context.Context, key string) error {
	return m.ProposeRaw(ctx, OpDelete, key, nil, 0)
}

// ProposeRaw proposes a raw value (already encoded by user's codec).
func (m *Manager[T]) ProposeRaw(ctx context.Context, op OpType, key string, value []byte, ttl time.Duration) error {
	if m.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	cmd := command{
		Op:    op,
		Key:   key,
		Value: value,
		TTL:   ttl,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := m.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	// Result from FSM.Apply
	res := future.Response()
	if res != nil {
		if err, ok := res.(error); ok {
			return err
		}
	}

	return nil
}

// Leader returns true if this node is the current Raft leader.
func (m *Manager[T]) Leader() bool {
	return m.raft.State() == raft.Leader
}

// Close gracefully stops the Raft node.
func (m *Manager[T]) Close() error {
	return m.raft.Shutdown().Error()
}

// AddPeer adds a new node to the cluster.
func (m *Manager[T]) AddPeer(id, addr string) error {
	future := m.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}
