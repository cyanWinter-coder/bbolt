
<hr>

## v1.6.0(TBD)

### BoltDB
- [Add support for `sparc64` arch](https://github.com/etcd-io/bbolt/pull/1292)
- [Add protection into MoveBucket to prevent cyclic move](https://github.com/etcd-io/bbolt/pull/1283)
- [Prevent Bucket from being copied by value](https://github.com/etcd-io/bbolt/pull/1300)
- Return pages allocated by a rolled-back read/write transaction to the free list instead of leaking them, and release pending pages promptly once the last referencing read-only transaction closes
- Make the hashmap freelist deterministic and fragmentation-aware (best-fit allocation, tie-break to the lowest page id), and compact the free list when the single-page-span ratio crosses the configured threshold
- Never reallocate pages still referenced by an open transaction, including the interleaved rollback case where a later transaction has already queued the page for freeing
- Add `Stats.ReusePageN` and keep `FreePageN`/`PendingPageN` consistent with the live free list; the in-memory reuse counter resets on reopen while the free-page accounting stays identical across restarts for both freelist sync modes

<hr>
