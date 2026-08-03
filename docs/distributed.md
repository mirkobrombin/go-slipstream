# Distributed State

Go-Slipstream has two separate distributed building blocks: Merkle root gossip for divergence detection and Raft for replicated writes.

## Merkle root gossip

Every engine maintains a local Merkle tree. `SyncManager` periodically publishes the local root through a go-warp sync bus and compares received roots with its own.

```go
import slipstreamsync "github.com/mirkobrombin/go-slipstream/pkg/sync"

manager := slipstreamsync.NewManager(engine, bus, 10*time.Second)
manager.Start(ctx)
```

The current `SyncManager` detects and logs divergence. It does not locate differing keys, transfer records, or reconcile nodes. Applications must not claim eventual convergence from this component alone.

Warp sync buses carry invalidation keys and metadata. They do not replicate Slipstream values. A service can combine Warp with the Slipstream store adapter, but it must define data ownership, routing, and conflict behavior explicitly.

## Raft replication

The Raft package replicates put and delete commands through Hashicorp Raft. The leader commits a command to the Raft log, then each node applies it to its local Slipstream engine.

The application API remains in-process. Slipstream does not provide an HTTP, gRPC, or RESP client endpoint for Raft leaders, and followers do not forward proposals. A remote deployment needs an application service that discovers or routes to the current leader.

Raft provides the implemented replicated-write path. Merkle root gossip is currently a detection mechanism, not an alternative replication protocol.
