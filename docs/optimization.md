# Performance & Optimization

Go-Slipstream is designed for efficiency. It utilizes several advanced techniques to minimize resource usage while maximizing throughput.

## Transparent Compression

Storage is often the bottleneck. Go-Slipstream integrates **Zstd** (Zstandard) compression directly into its pipeline.

- **How it works**: Data is compressed before hits the WAL and decompressed on-the-fly when read.
- **Benefits**:
    - Reduces Disk I/O (less data to move).
    - Drastically reduces the WAL file size (up to 70% for repetitive data).
    - Completely transparent to the application.

## Bloom Filters

To avoid expensive "Cold Reads" (searching for keys that don't exist), Go-Slipstream uses **Bloom Filters**.

- **Mechanism**: A probabilistic data structure that can tell if a key "might be" in the database or "definitely is not".
- **Implementation**: We use a 1MB bitset with 7 hash functions (by default) to provide a very low false positive rate.
- **Benefit**: If `bloom.MayContain(key)` returns `false`, Go-Slipstream skips the WAL seek/read entirely, saving a disk operation.

## Compaction Engine

Every update in a Bitcask engine appends new data, leaving the old data as "garbage" in the WAL. The **Compactor** solves this.

- **Process**:
    1. The Compactor scans the in-memory **KeyDir**.
    2. It copies only the latest "live" values into a new, temporary WAL file.
    3. It atomically swaps the old WAL with the new, compacted one.
- **Result**: The WAL size remains proportional to the amount of *live* data, not the total history of operations.

## Bare-Metal I/O (O_DIRECT)

Where supported (Linux), Go-Slipstream opens files with the `O_DIRECT` flag.
- **Zero Kernel Overhead**: Bypasses the kernel's Page Cache.
- **Direct-to-Disk**: Minimizes CPU usage and latency for write operations.
- **Alignment**: Requires writes to be aligned to the hardware page size (`4096 bytes`), which Go-Slipstream manages automatically via aligned buffers.
