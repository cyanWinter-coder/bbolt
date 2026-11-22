
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Add `CompactSnapshot`/`SnapshotCompactor`: build a compacted copy from a single source read transaction into a temporary file, validate it (meta checksums and bucket structure), and atomically publish it at the destination path, so readers only ever see a complete old or new snapshot

### CMD
- Add `--snapshot` flag to `bbolt compact` to publish the compacted copy atomically instead of writing the destination in place

<hr>
