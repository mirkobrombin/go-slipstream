# Distributed Anti-Entropy

Go-Slipstream is designed to live in a cluster. When integrated with [go-warp](https://github.com/mirkobrombin/go-warp), it provides a robust mechanism to ensure all nodes are in sync.

## Merkle Tree Sync

Every Go-Slipstream instance maintains a local **Merkle Tree**.
- Every operation updates a leaf in the tree.
- The **Root Hash** represents the unique state of the entire database.

### The Gossip Cycle
1. Every node periodically calculates its `MerkleRoot()`.
2. This hash is published to the `warp:syncbus` with a key like `slipstream:root:<node_id>`.
3. Other nodes receive this hash and compare it with their own root.
4. **Divergence Detected**: If the hashes don't match, the nodes identify the missing data and synchronize.

## Warp SyncManager

Go-Slipstream includes a `SyncManager` that simplifies this integration:

```go
import "github.com/mirkobrombin/go-slipstream/pkg/sync"

manager := sync.NewManager(engine, bus, 10*time.Second)
manager.Start(ctx) 
```

## Consistency Guarantees

While Go-Slipstream provides local ACID transactions, distributed consistency is handled via **Anti-Entropy**:
- **Eventually Consistent**: Nodes will converge to the same state after sync cycles.
- **Causal Ordering**: Leveraging Warp's Vector Clocks to ensure operations are applied in the correct order across the cluster.

## Strong Consistency (Raft)

For workloads requiring Strong Consistency (CP), Go-Slipstream provides a foundation for **Raft** integration.
- The `RaftManager` can be used to replicate WAL entries across a quorum of nodes before committing them locally.
- Note: Full Raft integration is an advanced feature currently in the foundational stage.
