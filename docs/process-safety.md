# Process Safety

A WAL directory belongs to one open `wal.Manager` process at a time. `NewManager` takes a non-blocking operating-system file lock on `.slipstream.lock` inside the directory.

If another process already owns the directory, `NewManager` returns an error that matches `wal.ErrDirectoryLocked` through `errors.Is`. It does not open a second in-memory index over the same WAL.

```go
manager, err := wal.NewManager("./data")
if errors.Is(err, wal.ErrDirectoryLocked) {
    return fmt.Errorf("storage is already running: %w", err)
}
if err != nil {
    return err
}
defer manager.Close()
```

`Close` releases the lock. The lock is also released by the operating system when a process exits or crashes, so the `.slipstream.lock` file may remain on disk without blocking a later process.

Processes that need shared access should expose one Slipstream owner through an application service. A shared filesystem directory is not a replication protocol. Raft-backed deployments must still give each node its own local WAL directory.
