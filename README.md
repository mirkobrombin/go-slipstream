<div align="center">
  <img src="https://github.com/mirkobrombin/go-slipstream/blob/main/logo.png?raw=true" height="128"/>
  <h1>go-slipstream</h1>
  <p>A high-performance, distributed embedded database for Go. It combines the simplicity of Bitcask with advanced industrial features like ACID transactions, secondary indexing, and transparent compression.</p>
  <p>
    <img src="https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go" alt="Go 1.25+">
    <img src="https://img.shields.io/badge/runtime_deps-none-success" alt="Zero runtime dependencies">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT">
  </p>
</div>

Built to be "Warp-Native", it serves as a persistence layer for [go-warp](https://github.com/mirkobrombin/go-warp), with `O_DIRECT` where supported, Raft replication, and Merkle root divergence detection.

- **Strong Consistency**: Integrated **Raft** consensus for linearizable operations across the cluster.
- **ACID Transactions**: Atomic `Begin/Commit/Rollback` operations with write-ahead protection.
- **Secondary Indexing**: Fast lookups on any field via in-memory inverted indices.

## Architecture

At its core, Go-Slipstream uses a **Bitcask-style** engine:
1. **WAL (Write-Ahead Log)**: All writes are sequential, providing high append performance
2. **KeyDir**: An in-memory hash map pointing to the latest offset of each key in the WAL
3. **Merkle Tree**: A persistent state hash updated on every write to detect divergence across nodes

## Performance

Slipstream is built for speed. By combining an append-only WAL with a high-speed L1 cache managed by `go-warp`, it achieves performance that often outclasses both embedded and remote databases in local-throughput scenarios.

### Why these comparisons?
Our benchmark suite compares Slipstream against diverse storage paradigms to provide a comprehensive "reality check":
- **Badger**: The industry standard for LSM-based embedded storage in Go
- **BoltDB**: The gold standard for B+Tree-based, read-heavy embedded storage
- **Redis & MongoDB**: Included to demonstrate that for local high-throughput workloads, an embedded database like Slipstream can eliminate the network overhead and serialization costs of remote servers

### Results (100k requests, C=50)

| System          | Write Ops/s | Read Ops/s   | Avg Lat (ns) | P99 Lat (ns) |
|:----------------|:------------|:-------------|:-------------|:-------------|
| **slipstream**  | **247,269** | **1,887,153**| 104,060      | 11,899,301   |
| badger          | 250,514     | 768,208      | 128,577      | 3,763,804    |
| bolt            | 30,718      | 585,707      | 846,869      | 3,113,259    |
| redis           | 119,586     | 129,860      | 400,740      | 40,343,851   |
| mongodb         | 24,110*     | 49,120*      | 15.4ms*      | 188ms*       |

*\*Benchmarks performed on local hardware. Results may vary based on disk I/O and CPU.*

### Running Benchmarks
We provide an automated script to run the full suite (including Docker/Podman setup for remote targets):
```bash
./examples/bench/run_benchmarks.sh
```
Or run individual targets via CLI:
```bash
go run ./examples/bench -target slipstream -n 100000 -c 50
```

## Documentation

- **[Architecture Overview](docs/architecture.md)**: WAL, O_DIRECT, and the Bitcask engine.
- **[Raft Consensus](docs/raft.md)**: Strong consistency and leader election.
- **[ACID Transactions](docs/acid.md)**: Understanding the transaction model and markers.
- **[Process Safety](docs/process-safety.md)**: Exclusive directory ownership across processes.
- **[Secondary Indices](docs/indexes.md)**: Querying by fields other than the primary key.
- **[Performance & Optimization](docs/optimization.md)**: Bloom Filters, Compression, and Compaction.
- **[Warp Integration](docs/distributed.md)**: Distributed anti-entropy and syncbus gossip.

## Installation

```bash
go get github.com/mirkobrombin/go-slipstream
```

## Quick Start

### Basic Key-Value
```go
package main

import (
    "context"
    "github.com/mirkobrombin/go-slipstream/pkg/engine"
    "github.com/mirkobrombin/go-slipstream/pkg/wal"
)

func main() {
    w, _ := wal.NewManager("data")
    defer w.Close()
    db := engine.NewBitcask[string](w, 
        func(s string) ([]byte, error) { return []byte(s), nil },
        func(b []byte) (string, error) { return string(b), nil },
    )
    
    ctx := context.Background()

    db.Put(ctx, "hello", "world", 0)
    val, _ := db.Get(ctx, "hello")
}
```

### ACID Transactions
```go
tx, _ := db.Begin()
tx.Put(ctx, "balance:A", "90", 0)
tx.Put(ctx, "balance:B", "110", 0)
tx.Commit(ctx) // Atomic
```

### Conditional Writes
```go
err := db.PutIf(ctx, "account:1", next, 0,
    func(current Account, exists bool) bool {
        return exists && current.Version == expectedVersion
    },
)
```

### Secondary Indices
```go
// Register an index on the "city" field
engine.AddIndex("city", func(u User) string { return u.City })

// Fast lookup with fluent API
users, _ := db.GetByIndex(ctx, "city", "Torino").
    Limit(10).
    All()
```

## License

MIT License. See [LICENSE](LICENSE) for details.
