# Transactions and Conditional Writes

Go-Slipstream buffers transaction operations in memory and appends them to the WAL under one transaction ID. A commit marker follows the operations. Recovery applies the operations only when that marker is present, so a crash before the marker leaves no partial transaction state.

## Transaction model

1. `Begin` allocates a transaction ID.
2. `Put` and `Delete` buffer operations. A transaction reads its own buffered writes.
3. `Commit` appends the operations and commit marker, syncs the WAL, then publishes the new in-memory state.
4. `Rollback` discards the buffer without a WAL write.

The engine provides read-committed isolation. Transactions do not track a general read set and do not detect conflicts unless commit-time conditions are declared.

```go
transaction, err := db.Begin()
if err != nil {
    return err
}

if err := transaction.Put(ctx, "balance:A", Account{Balance: 90}, 0); err != nil {
    return err
}
if err := transaction.Put(ctx, "balance:B", Account{Balance: 110}, 0); err != nil {
    return err
}
return transaction.Commit(ctx)
```

## Atomic conditional writes

`PutIfAbsent` permits one winner when concurrent writers create the same key. `PutIf` and `DeleteIf` evaluate a predicate against the committed value while the engine write lock is held. A false predicate returns `tx.ErrConditionFailed` without a WAL write.

Conditions must be deterministic, fast, and side-effect free. They must not call back into the same engine because the commit lock is already held.

```go
err := db.PutIf(ctx, "account:1", next, 0,
    func(current Account, exists bool) bool {
        return exists && current.Version == expectedVersion
    },
)
```

The version in this example belongs to the application value. This lets each domain define its own version or generation field without a storage format dependency.

## Commit-time conditions

`BeginConditional` returns a transaction that supports `Require`. All requirements are evaluated against committed state immediately before the first WAL append. Requirements use AND semantics. Buffered writes do not change their inputs.

```go
transaction, err := db.BeginConditional()
if err != nil {
    return err
}

err = transaction.Require("project:42", func(project Project, exists bool) bool {
    return exists && project.Version == expectedProjectVersion
})
if err != nil {
    return err
}
err = transaction.Require("project:42:relations", func(guard Guard, exists bool) bool {
    return exists && guard.Generation == expectedGeneration
})
if err != nil {
    return err
}

if err := transaction.Put(ctx, "relation:42:memory:7", relation, 0); err != nil {
    return err
}
if err := transaction.Put(ctx, "project:42:relations", nextGuard, 0); err != nil {
    return err
}
return transaction.Commit(ctx)
```

Every operation that creates or deletes a relationship must update the same guard key. Parent deletion requires the parent version and the guard generation. This makes relationship creation and protected parent deletion conflict under the same lock.

## Error handling

- `engine.ErrKeyExists` reports a failed `PutIfAbsent`.
- `tx.ErrConditionFailed` reports a false direct or transaction condition.
- `tx.ErrDone` reports use after commit or rollback.
- `tx.ErrCommitUncertain` means the commit marker may have reached storage but WAL sync failed. Reopen and recover before deciding whether to retry.
