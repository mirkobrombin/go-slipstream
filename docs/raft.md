# Raft Consensus

Go-Slipstream integrates **hashicorp/raft** to provide strong consistency across a cluster of nodes.

## Architecture

When Raft is enabled, writes follow a strict path:
1. **Proposal**: A client sends a write request (`Put` or `Delete`) to the Raft Leader.
2. **Replication**: The Leader appends the entry to its log and replicates it to followers.
3. **Commit**: Once a quorum (majority) of nodes acknowledge the log entry, it is considered committed.
4. **Application**: The `FSM` on each node applies the committed entry to the local Slipstream Engine.

## Configuration

```go
import "github.com/mirkobrombin/go-slipstream/pkg/raft"

cfg := &raft.Config{
    NodeID:    "node-1",
    BindAddr:  "127.0.0.1:40001",
    DataDir:   "/var/lib/slipstream/raft",
    Bootstrap: true,
}

rm, _ := raft.NewManager(engine, encode, decode, cfg)
```

## Operations

- **Propose**: Linearizable write. Must be called on the Leader.
- **Leader()**: Checks if the local node is currently the leader.
- **AddPeer/RemovePeer**: Dynamicaly modify the cluster membership.

## Data Persistence

Raft state is persisted using:
- **Log Store/Stable Store**: Internal Slipstream Engine (`store/`)
- **Snapshots**: Local files in `DataDir`.

Snapshots are automatically taken by Raft to prune logs. Slipstream implements full state serialization for snapshots, ensuring new nodes can catch up quickly.
