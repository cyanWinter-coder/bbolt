
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Add transactional batch bucket move via `Tx.MoveBuckets` and `Bucket.MoveBuckets`: all moves are pre-validated (duplicate target, key conflict, cross-database, self-move and ancestor/descendant cycle) before any visible change, and are applied atomically within the current write transaction while preserving bucket pointers, sequences and sub-bucket caches. Failures are reported per item through `BucketMoveError`.

<hr>
