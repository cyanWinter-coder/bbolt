
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Add interruptible, observable online compaction: `CompactWithOptions`/`Compactor` copy from a single consistent source read transaction in size-bounded write batches with progress callbacks (relocated pages and remaining ratio), preflight an absent destination and free disk space, build in a temporary file and publish atomically, cleaning up on cancellation or failure.

<hr>
