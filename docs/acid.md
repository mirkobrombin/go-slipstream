# ACID Transactions

Go-Slipstream provides full ACID (Atomicity, Consistency, Isolation, Durability) guarantees for multi-key operations via its transaction API.

## Transaction Model

Transactions in Go-Slipstream are **Optimistic** and **Buffered**:
1. **Begin**: Starts a new transaction with a unique `TxID`.
2. **Operations**: `Put` and `Delete` are buffered in memory and not yet visible to other readers.
3. **Commit**: All operations are flushed to the WAL sequentially, followed by a special **Commit Marker**.
4. **Rollback**: Clears the buffers without touching the disk or KeyDir.

## The Atomic Commit Marker

The secret to Go-Slipstream's reliability is the **Commit Marker** in the WAL.
- During recovery (not yet fully implemented in this POC, but the foundation is there), the engine scans the WAL.
- Entries belonging to a `TxID` are only applied if a matching `Commit` entry is found at the end of the sequence.
- This ensures that partial writes (e.g., due to power failure) never corrupt the database state.

## Usage Example

```go
tx, err := db.Begin()
if err != nil {
    return err
}

tx.Put(ctx, "user:1:wallet", "500")
tx.Put(ctx, "user:2:wallet", "1500")

if someError {
    tx.Rollback()
    return
}

err = tx.Commit(ctx)
```

## Isolation Level

Go-Slipstream currently implements **Read-Committed** isolation. Operations within a transaction are only visible to the rest of the system once `Commit()` is successfully completed.

## Performance Impact

Because transactions buffer operations in memory, they are extremely fast. A `Commit()` performs a sequential write of all buffered entries, which is significantly more efficient than multiple individual `Put` operations during high-load scenarios.
