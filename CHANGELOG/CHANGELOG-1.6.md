
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Add `Bucket.PrefixCursor`, a resumable prefix iterator with stable traversal semantics while deleting keys in the current transaction; deleting the current key continues from the next original ordered position instead of skipping or revisiting records after node rebalancing, and closing the transaction or deleting the iterated bucket returns a recognizable `ErrCursorTerminal` state.

<hr>
