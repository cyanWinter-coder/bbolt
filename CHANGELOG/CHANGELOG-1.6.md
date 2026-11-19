
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Add `DB.BatchContext`, a context-aware variant of `DB.Batch`: a canceled
  context makes the caller stop waiting for the batch result without
  interrupting the batch or blocking other callers; `DB.Batch` is now a thin
  wrapper over it with `context.Background()` and keeps its existing behavior

<hr>
