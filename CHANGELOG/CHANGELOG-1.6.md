
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Fix reverse cursor iteration (`Prev`) terminating early or revisiting keys when leaf pages are emptied by deletes
- Fix `Cursor.Delete()` followed by `Next()` skipping the successor of the removed key; `Next`/`Prev` now land on the successor/predecessor without duplicates or gaps
- Return `ErrTxClosed` from `Cursor` methods instead of panicking or reading released pages after the owning transaction is committed or rolled back
- Add `Cursor.SeekPrefix(prefix, end)` for prefix scans with an exclusive end boundary
- **Breaking API change:** `Cursor.First/Last/Next/Prev/Seek` now return an additional `error` value as their last return value

<hr>
