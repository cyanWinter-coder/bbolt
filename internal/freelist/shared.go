package freelist

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

type txPending struct {
	ids              []common.Pgid
	alloctx          []common.Txid // txids allocating the ids
	lastReleaseBegin common.Txid   // beginning txid of last matching releaseRange
}

// allocEntry records a contiguous span of pages handed out by Allocate.
// size is always >= 1; the span covers [start, start+size).
type allocEntry struct {
	txid common.Txid // transaction the span was allocated to
	size uint64      // number of pages in the span
}

// pgidSpan is a contiguous range of free pages [start, start+size).
type pgidSpan struct {
	start common.Pgid
	size  uint64
}

type shared struct {
	Interface

	readonlyTXIDs []common.Txid               // all readonly transaction IDs.
	allocs        map[common.Pgid]*allocEntry // mapping of spans allocated out of the free list by starting pgid.
	cache         map[common.Pgid]struct{}    // fast lookup of all free and pending page ids.
	pending       map[common.Txid]*txPending  // mapping of soon-to-be free page ids by tx.
	reuseCount    atomic.Uint64               // number of successful allocations served from the free list.
}

func newShared() *shared {
	return &shared{
		pending: make(map[common.Txid]*txPending),
		allocs:  make(map[common.Pgid]*allocEntry),
		cache:   make(map[common.Pgid]struct{}),
	}
}

func (t *shared) pendingPageIds() map[common.Txid]*txPending {
	return t.pending
}

func (t *shared) PendingCount() int {
	var count int
	for _, txp := range t.pending {
		count += len(txp.ids)
	}
	return count
}

func (t *shared) Count() int {
	return t.FreeCount() + t.PendingCount()
}

func (t *shared) Freed(pgId common.Pgid) bool {
	_, ok := t.cache[pgId]
	return ok
}

func (t *shared) Free(txid common.Txid, p *common.Page) {
	if p.Id() <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.Id()))
	}

	// Free page and all its overflow pages.
	txp := t.pending[txid]
	if txp == nil {
		txp = &txPending{}
		t.pending[txid] = txp
	}
	allocEntry, ok := t.allocs[p.Id()]
	var allocTxid common.Txid
	if ok {
		allocTxid = allocEntry.txid
	}
	common.Verify(func() {
		if allocTxid == txid {
			panic(fmt.Sprintf("free: freed page (%d) was allocated by the same transaction (%d)", p.Id(), txid))
		}
	})
	if ok {
		delete(t.allocs, p.Id())
	}

	for id := p.Id(); id <= p.Id()+common.Pgid(p.Overflow()); id++ {
		// Verify that page is not already free.
		if _, ok := t.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		t.cache[id] = struct{}{}
	}
}

// recordAlloc stores an allocation served from the free list so that it can
// be restored if the allocating transaction rolls back.
func (t *shared) recordAlloc(txid common.Txid, start common.Pgid, size uint64) {
	t.allocs[start] = &allocEntry{txid: txid, size: size}
	t.reuseCount.Add(1)
}

// ReuseCount returns the number of allocations served from the free list
// since the free list was created.
func (t *shared) ReuseCount() uint64 {
	return t.reuseCount.Load()
}

// Compact rebuilds the free page set so that adjacent pages form single
// maximal spans. It only performs the rebuild when the fraction of
// single-page spans reaches fragmentRatio and the free list holds at least
// minFreePages pages.
func (t *shared) Compact(fragmentRatio float64, minFreePages int) bool {
	if fragmentRatio <= 0 || fragmentRatio > 1 {
		panic(fmt.Sprintf("fragmentRatio must be in (0.0, 1.0], got %v", fragmentRatio))
	}
	if minFreePages <= 0 {
		panic(fmt.Sprintf("minFreePages must be positive, got %d", minFreePages))
	}

	freeCount := t.FreeCount()
	if freeCount < minFreePages {
		return false
	}

	spanCount := t.freeSpanCount()
	if spanCount == 0 {
		return false
	}
	// The worst possible fragmentation is one page per span.
	singlePageSpans := spanCount - t.multiPageSpanCount()
	if float64(singlePageSpans)/float64(spanCount) < fragmentRatio {
		return false
	}

	// Rebuild from the authoritative page id list. Init collapses adjacent
	// ids into single spans, which also keeps all lookup structures in sync.
	t.Init(t.freePageIds())
	return true
}

func (t *shared) Rollback(txid common.Txid) {
	// orphaned collects pending pages whose allocating transaction no longer
	// exists (it already rolled back); they must return to the free set.
	var orphaned []common.Pgid

	// Remove page ids from cache.
	if txp := t.pending[txid]; txp != nil {
		for i, pgid := range txp.ids {
			delete(t.cache, pgid)
			tx := txp.alloctx[i]
			if tx == 0 {
				// The page was never allocated through the free list (e.g. a
				// page freshly appended to the file), so there is no span to
				// restore. Skip it.
				continue
			}
			if tx != txid {
				if _, stillTracked := t.allocs[pgid]; stillTracked {
					// Pending free aborted; restore page back to alloc list.
					// The span size is unknown here because the pending list
					// only tracks individual page ids; record a single-page
					// entry, which is sufficient for subsequent Free/Rollback
					// bookkeeping.
					t.allocs[pgid] = &allocEntry{txid: tx, size: 1}
				} else {
					// The allocating transaction already rolled back and
					// dropped its span tracking, so nobody owns this page
					// anymore; it is free once this pending free is undone.
					orphaned = append(orphaned, pgid)
				}
			} else {
				// A writing TXN should never free a page which was allocated by itself.
				panic(fmt.Sprintf("rollback: freed page (%d) was allocated by the same transaction (%d)", pgid, txid))
			}
		}
		// Remove pages from pending list.
		delete(t.pending, txid)
	}

	// Restore the spans allocated by this transaction back into the free set.
	// Pages which were allocated by the same transaction must not remain in
	// the cache while they are re-added, so detach them first and let
	// mergeSpans re-insert them together with their neighbours.
	rolledBack := make([]pgidSpan, 0)
	for start, entry := range t.allocs {
		if entry.txid != txid {
			continue
		}
		rolledBack = append(rolledBack, pgidSpan{start: start, size: entry.size})
		// The allocation is undone, so its reuse no longer counts.
		t.reuseCount.Add(^uint64(0))
		for i := common.Pgid(0); i < common.Pgid(entry.size); i++ {
			delete(t.cache, start+i)
		}
	}
	// Build a lookup of every page currently held in a pending free list so
	// the span restoration below can skip pages another transaction still
	// references.
	pendingLookup := t.pendingPgidSet()
	for _, span := range rolledBack {
		delete(t.allocs, span.start)
		ids := make([]common.Pgid, 0, span.size)
		for i := common.Pgid(0); i < common.Pgid(span.size); i++ {
			pgid := span.start + i
			// Another transaction may already be holding this page in its
			// pending free list. Such a page must not re-enter the free set
			// while it is still pending; it stays in the cache via reindex.
			if _, ok := pendingLookup[pgid]; ok {
				continue
			}
			ids = append(ids, pgid)
		}
		if len(ids) > 0 {
			t.mergeSpans(ids)
		}
	}

	// Return pending pages whose allocating transaction has already gone.
	if len(orphaned) > 0 {
		t.mergeSpans(orphaned)
	}

	// Rebuild the lookup cache: the restored pages were detached from the
	// cache while allocated, and mergeSpans only updates the backend's span
	// structures, not the shared cache.
	t.reindex()
}

// pendingPgidSet returns a lookup set of every page currently in a
// transaction's pending free list.
func (t *shared) pendingPgidSet() map[common.Pgid]struct{} {
	pendingLookup := make(map[common.Pgid]struct{})
	for _, txp := range t.pending {
		for _, id := range txp.ids {
			pendingLookup[id] = struct{}{}
		}
	}
	return pendingLookup
}

func (t *shared) AddReadonlyTXID(tid common.Txid) {
	t.readonlyTXIDs = append(t.readonlyTXIDs, tid)
}

func (t *shared) RemoveReadonlyTXID(tid common.Txid) {
	for i := range t.readonlyTXIDs {
		if t.readonlyTXIDs[i] == tid {
			last := len(t.readonlyTXIDs) - 1
			t.readonlyTXIDs[i] = t.readonlyTXIDs[last]
			t.readonlyTXIDs = t.readonlyTXIDs[:last]
			break
		}
	}
}

type txIDx []common.Txid

func (t txIDx) Len() int           { return len(t) }
func (t txIDx) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t txIDx) Less(i, j int) bool { return t[i] < t[j] }

func (t *shared) ReleasePendingPages() {
	// Free all pending pages prior to the earliest open transaction.
	sort.Sort(txIDx(t.readonlyTXIDs))
	minid := common.Txid(math.MaxUint64)
	if len(t.readonlyTXIDs) > 0 {
		minid = t.readonlyTXIDs[0]
	}
	if minid > 0 {
		t.release(minid - 1)
	}
	// Release unused txid extents.
	for _, tid := range t.readonlyTXIDs {
		t.releaseRange(minid, tid-1)
		minid = tid + 1
	}
	t.releaseRange(minid, common.Txid(math.MaxUint64))
	// Any page both allocated and freed in an extent is safe to release.
}

func (t *shared) release(txid common.Txid) {
	m := make(common.Pgids, 0)
	for tid, txp := range t.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(t.pending, tid)
		}
	}
	t.mergeSpans(m)
}

func (t *shared) releaseRange(begin, end common.Txid) {
	if begin > end {
		return
	}
	m := common.Pgids{}
	for tid, txp := range t.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(t.pending, tid)
		}
	}
	t.mergeSpans(m)
}

// Copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (t *shared) Copyall(dst []common.Pgid) {
	m := make(common.Pgids, 0, t.PendingCount())
	for _, txp := range t.pendingPageIds() {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	common.Mergepgids(dst, t.freePageIds(), m)
}

func (t *shared) Reload(p *common.Page) {
	t.Read(p)
	t.NoSyncReload(t.freePageIds())
}

func (t *shared) NoSyncReload(pgIds common.Pgids) {
	// Build a cache of only pending pages.
	pcache := make(map[common.Pgid]struct{})
	for _, txp := range t.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = struct{}{}
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	a := []common.Pgid{}
	for _, id := range pgIds {
		if _, ok := pcache[id]; !ok {
			a = append(a, id)
		}
	}

	t.Init(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (t *shared) reindex() {
	free := t.freePageIds()
	pending := t.pendingPageIds()
	t.cache = make(map[common.Pgid]struct{}, len(free))
	for _, id := range free {
		t.cache[id] = struct{}{}
	}
	for _, txp := range pending {
		for _, pendingID := range txp.ids {
			t.cache[pendingID] = struct{}{}
		}
	}
}

func (t *shared) Read(p *common.Page) {
	if !p.IsFreelistPage() {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.Id(), p.Typ()))
	}

	ids := p.FreelistPageIds()

	// Copy the list of page ids from the freelist.
	if len(ids) == 0 {
		t.Init([]common.Pgid{})
	} else {
		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]common.Pgid, len(ids))
		copy(idsCopy, ids)
		// Make sure they're sorted.
		sort.Sort(common.Pgids(idsCopy))

		t.Init(idsCopy)
	}
}

func (t *shared) EstimatedWritePageSize() int {
	n := t.Count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return int(common.PageHeaderSize) + (int(unsafe.Sizeof(common.Pgid(0))) * n)
}

func (t *shared) Write(p *common.Page) {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.SetFlags(common.FreelistPageFlag)

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := t.Count()
	if l == 0 {
		p.SetCount(uint16(l))
	} else if l < 0xFFFF {
		p.SetCount(uint16(l))
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l)
		t.Copyall(ids)
	} else {
		p.SetCount(0xFFFF)
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l+1)
		ids[0] = common.Pgid(l)
		t.Copyall(ids[1:])
	}
}
