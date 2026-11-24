package freelist

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// TestFreelist_Rollback_RestoresAllocatedSpans verifies that every span
// handed out by Allocate is returned to the free set when the allocating
// transaction rolls back, including multi-page (overflow) spans.
func TestFreelist_Rollback_RestoresAllocatedSpans(t *testing.T) {
	for _, name := range []string{"array", "hashmap"} {
		t.Run(name, func(t *testing.T) {
			var f Interface
			if name == "hashmap" {
				f = NewHashMapFreelist()
			} else {
				f = NewArrayFreelist()
			}
			// Two spans: [10..19] and [30..34].
			ids := make(common.Pgids, 0, 15)
			for i := common.Pgid(10); i <= 19; i++ {
				ids = append(ids, i)
			}
			for i := common.Pgid(30); i <= 34; i++ {
				ids = append(ids, i)
			}
			f.Init(ids)
			require.Equal(t, 15, f.FreeCount())
			require.Equal(t, 0, int(f.ReuseCount()))

			// Allocate a 4-page span and a single page within tx 5.
			p1 := f.Allocate(5, 4)
			p2 := f.Allocate(5, 1)
			require.NotZero(t, p1)
			require.NotZero(t, p2)
			require.Equal(t, 2, int(f.ReuseCount()))
			require.Equal(t, 10, f.FreeCount())

			// Rollback must restore every page, even the multi-page span.
			f.Rollback(5)
			require.Equal(t, 15, f.FreeCount())
			require.Equal(t, 0, int(f.ReuseCount()))
			require.Equal(t, ids, f.freePageIds())
			require.Empty(t, f.pendingPageIds())

			// The restored spans must be allocatable again.
			require.Equal(t, p1, f.Allocate(6, 4))
			require.Equal(t, 1, int(f.ReuseCount()))
			f.Rollback(6)
			require.Equal(t, 0, int(f.ReuseCount()))
		})
	}
}

// TestFreelist_Rollback_OnlyRestoresRolledBackTx ensures pages allocated by
// other, still live transactions are never touched by a rollback.
func TestFreelist_Rollback_OnlyRestoresRolledBackTx(t *testing.T) {
	f := newTestFreelist()
	// Two independent, non-adjacent spans of two pages each.
	ids := common.Pgids{10, 11, 20, 21}
	f.Init(ids)

	// tx 100 takes one span, tx 101 takes the other.
	a := f.Allocate(100, 2)
	b := f.Allocate(101, 2)
	require.NotZero(t, a)
	require.NotZero(t, b)
	require.NotEqual(t, a, b)
	require.Equal(t, 2, int(f.ReuseCount()))
	require.Equal(t, 0, f.FreeCount())

	// Rolling back tx 100 must only return its own span.
	f.Rollback(100)
	require.Equal(t, 2, f.FreeCount())
	require.Equal(t, 1, int(f.ReuseCount()))
	require.False(t, f.Freed(b), "span of the live tx 101 must not be freed")
	for i := 0; i < 2; i++ {
		require.True(t, f.Freed(a+common.Pgid(i)))
	}
}

// TestFreelist_Hashmap_BestFit verifies the hashmap backend picks the
// tightest fitting span (and the lowest start id on ties) instead of a random
// larger span, which limits fragmentation.
func TestFreelist_Hashmap_BestFit(t *testing.T) {
	f := NewHashMapFreelist()
	// Spans of size 4 at 10, size 2 at 20 and size 1 at 30.
	f.Init(common.Pgids{10, 11, 12, 13, 20, 21, 30})

	// A 2-page request must take the exact size-2 span at 20, not split the
	// size-4 span at 10.
	require.Equal(t, common.Pgid(20), f.Allocate(1, 2))
	require.Equal(t, common.Pgids{10, 11, 12, 13, 30}, f.freePageIds())

	// A 3-page request has no exact match and must split the size-4 span.
	require.Equal(t, common.Pgid(10), f.Allocate(1, 3))
	require.Equal(t, common.Pgids{13, 30}, f.freePageIds(),
		"the one-page remainder stays available")

	// On ties the lowest starting page id wins deterministically, including
	// for the exact-size fast path. Run repeatedly since a map-iteration
	// based implementation would return either span non-deterministically.
	f2 := NewHashMapFreelist()
	f2.Init(common.Pgids{40, 41, 50, 51})
	for i := 0; i < 20; i++ {
		require.Equal(t, common.Pgid(40), f2.Allocate(1, 2),
			"exact-size ties must resolve to the lowest start id")
		f2.Rollback(1)
	}
}

// TestFreelist_Rollback_CrossTxPending verifies interleaved rollbacks keep
// pending and free sets disjoint: one transaction allocates two spans, a
// later transaction frees one (pending), and the allocating transaction rolls
// back before the freeing one does.
func TestFreelist_Rollback_CrossTxPending(t *testing.T) {
	for _, name := range []string{"array", "hashmap"} {
		t.Run(name, func(t *testing.T) {
			var f Interface
			if name == "hashmap" {
				f = NewHashMapFreelist()
			} else {
				f = NewArrayFreelist()
			}
			f.Init(common.Pgids{10, 11, 20, 21})
			s1 := f.Allocate(5, 2)
			s2 := f.Allocate(5, 2)
			require.ElementsMatch(t, []common.Pgid{10, 20}, []common.Pgid{s1, s2})

			// tx 6 frees one span by its start page (two overflow pages).
			f.Free(6, common.NewPage(s2, common.LeafPageFlag, 0, 1))
			require.Equal(t, 2, f.PendingCount())

			// tx 5 rolls back: only the still-allocated span returns to free.
			f.Rollback(5)
			require.Equal(t, 2, f.FreeCount())
			require.Equal(t, 2, f.PendingCount())

			// The pages must never be both free and pending.
			for _, pgid := range f.freePageIds() {
				for _, txp := range f.pendingPageIds() {
					for _, pendingID := range txp.ids {
						require.NotEqual(t, pendingID, pgid)
					}
				}
			}

			// tx 6 rollback releases the rest, restoring the original state.
			f.Rollback(6)
			require.Equal(t, 4, f.FreeCount())
			require.Equal(t, 0, f.PendingCount())
			require.Equal(t, common.Pgids{10, 11, 20, 21}, f.freePageIds())
		})
	}
}

// TestFreelist_ReuseCount verifies the in-memory reuse counter semantics.
func TestFreelist_ReuseCount(t *testing.T) {
	f := newTestFreelist()
	f.Init(common.Pgids{10, 11, 12, 20})

	require.Equal(t, uint64(0), f.ReuseCount())
	f.Allocate(1, 2)
	f.Allocate(1, 1)
	require.Equal(t, uint64(2), f.ReuseCount())

	// Requests which cannot be served do not count as reuse.
	require.Zero(t, f.Allocate(1, 5))
	require.Equal(t, uint64(2), f.ReuseCount())
	require.Zero(t, f.Allocate(1, 0))
	require.Equal(t, uint64(2), f.ReuseCount())

	f.Rollback(1)
	require.Equal(t, uint64(0), f.ReuseCount())
}

// TestFreelist_Compact verifies the fragmentation threshold behaviour.
func TestFreelist_Compact(t *testing.T) {
	for _, name := range []string{"array", "hashmap"} {
		t.Run(name, func(t *testing.T) {
			var f Interface
			if name == "hashmap" {
				f = NewHashMapFreelist()
			} else {
				f = NewArrayFreelist()
			}
			// Four isolated single-page spans.
			f.Init(common.Pgids{10, 20, 30, 40})

			// Below the minimum free page count: nothing happens.
			require.False(t, f.Compact(0.5, 1000))
			require.Equal(t, 4, f.freeSpanCount())

			// 100% single-page spans >= 0.5 ratio: compaction runs.
			require.True(t, f.Compact(0.5, 4))
			// The pages are not adjacent on disk, so the span count does not
			// change, but the lookup structures must remain consistent.
			require.Equal(t, 4, f.freeSpanCount())
			require.Equal(t, common.Pgids{10, 20, 30, 40}, f.freePageIds())

			// Adjacent ids collapse into a single span after compaction.
			f.Init(common.Pgids{10, 11, 12, 13, 14})
			// Fragment the span through partial allocations and rollbacks.
			f.Allocate(1, 1)
			f.Allocate(1, 1)
			f.Rollback(1)
			require.Equal(t, 1, f.freeSpanCount(),
				"rollback re-merges adjacent spans through mergeSpans")

			// Invalid arguments must panic.
			require.Panics(t, func() { f.Compact(0, 4) })
			require.Panics(t, func() { f.Compact(1.5, 4) })
			require.Panics(t, func() { f.Compact(0.5, 0) })
		})
	}
}

// TestFreelist_Compact_MergesFragmentedSpans verifies that a rebuild after
// mixed allocations coalesces adjacent spans again.
func TestFreelist_Compact_MergesFragmentedSpans(t *testing.T) {
	f := newTestFreelist()
	// One contiguous span of 8 pages.
	f.Init(common.Pgids{10, 11, 12, 13, 14, 15, 16, 17})

	// Allocate from the middle pattern: take 1 page three times, which leaves
	// fragmented remainders, then roll back to make them free again without
	// relying on merge order.
	f.Allocate(1, 1)
	f.Allocate(1, 1)
	f.Allocate(1, 1)
	f.Rollback(1)
	require.Equal(t, common.Pgids{10, 11, 12, 13, 14, 15, 16, 17}, f.freePageIds())
	require.Equal(t, 1, f.freeSpanCount())
}
