package bbolt_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// bucketMoveExpectation describes where a bucket is expected to live after a
// batch move: present under a parent path (or at root when the path is empty)
// or absent everywhere it used to be.
type bucketMoveExpectation struct {
	key       string
	parent    []string // empty means root
	present   bool
	sequence  uint64
	dataKey   []byte
	dataValue []byte
}

// setupBatchMoveDB builds a deterministic tree used across the batch tests:
//
//	root/
//	  a/
//	    b/            (sequence 7, holds key "k1"="v1", has nested child "deep")
//	      deep/       (holds key "dk"="dv")
//	  x/
//	  y/
//	  z/              (holds a non-bucket key "conflict"="plain")
func setupBatchMoveDB(t *testing.T) *btesting.DB {
	t.Helper()
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		a, err := tx.CreateBucket([]byte("a"))
		if err != nil {
			return err
		}
		b, err := a.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		if err := b.SetSequence(7); err != nil {
			return err
		}
		if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
			return err
		}
		deep, err := b.CreateBucket([]byte("deep"))
		if err != nil {
			return err
		}
		if err := deep.Put([]byte("dk"), []byte("dv")); err != nil {
			return err
		}
		for _, name := range []string{"x", "y"} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		z, err := tx.CreateBucket([]byte("z"))
		if err != nil {
			return err
		}
		return z.Put([]byte("conflict"), []byte("plain"))
	}))
	return db
}

func openPath(tx *bbolt.Tx, path []string) *bbolt.Bucket {
	var b *bbolt.Bucket
	for _, p := range path {
		if b == nil {
			b = tx.Bucket([]byte(p))
		} else {
			b = b.Bucket([]byte(p))
		}
		if b == nil {
			return nil
		}
	}
	return b
}

// verifyBucketLocation asserts a bucket is present (or absent) at the
// expected parent and that its sequence/data survived the move.
func verifyBucketLocation(t *testing.T, tx *bbolt.Tx, e bucketMoveExpectation) {
	t.Helper()
	parent := openPath(tx, e.parent)
	if !e.present {
		if parent != nil {
			require.Nil(t, parent.Bucket([]byte(e.key)),
				"bucket %q must be gone from %v", e.key, e.parent)
		}
		return
	}
	require.NotNil(t, parent, "parent %v must exist", e.parent)
	child := parent.Bucket([]byte(e.key))
	require.NotNil(t, child, "bucket %q must exist under %v", e.key, e.parent)
	require.Equal(t, e.sequence, child.Sequence())
	require.Equal(t, e.dataValue, child.Get(e.dataKey))
}

func TestTx_MoveBuckets_Success(t *testing.T) {
	db := setupBatchMoveDB(t)

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		x := tx.Bucket([]byte("x"))

		specs := []bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: a, Dst: x},
			{Key: []byte("y"), Src: nil, Dst: x},
		}
		return tx.MoveBuckets(specs)
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		// b now lives under x, with sequence, data and nested child intact.
		verifyBucketLocation(t, tx, bucketMoveExpectation{
			key: "b", parent: []string{"x"}, present: true,
			sequence: 7, dataKey: []byte("k1"), dataValue: []byte("v1"),
		})
		x := tx.Bucket([]byte("x"))
		deep := x.Bucket([]byte("b")).Bucket([]byte("deep"))
		require.NotNil(t, deep)
		require.Equal(t, []byte("dv"), deep.Get([]byte("dk")))

		// y moved under x.
		verifyBucketLocation(t, tx, bucketMoveExpectation{
			key: "y", parent: []string{"x"}, present: true,
		})

		// a no longer contains b, and y is no longer top level.
		require.Nil(t, tx.Bucket([]byte("a")).Bucket([]byte("b")))
		require.Nil(t, tx.Bucket([]byte("y")))
		return nil
	}))
}

func TestTx_MoveBuckets_EmptyIsNoOp(t *testing.T) {
	db := setupBatchMoveDB(t)
	before := dumpDB(t, db)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.MoveBuckets(nil)
	}))
	after := dumpDB(t, db)
	require.Equal(t, before, after)
}

func TestTx_MoveBuckets_RequiresWritableTx(t *testing.T) {
	db := setupBatchMoveDB(t)
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		err := tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
		})
		require.Error(t, err)
		var bme *bbolt.BucketMoveError
		require.True(t, errors.As(err, &bme))
		require.ErrorIs(t, err, berrors.ErrTxNotWritable)
		return nil
	}))
}

// TestTx_MoveBuckets_PrecheckFailureLeavesStructureUntouched covers every
// precheck failure mode and asserts that no earlier spec in the same batch
// was applied when a later spec is invalid.
func TestTx_MoveBuckets_PrecheckFailureLeavesStructureUntouched(t *testing.T) {
	testCases := []struct {
		name        string
		buildSpecs  func(tx *bbolt.Tx) []bbolt.MoveBucketSpec
		expectedErr error
		expectedIdx int
		expectedKey string
	}{
		{
			name: "bucket not found",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("missing"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("y"))},
				}
			},
			expectedErr: berrors.ErrBucketNotFound,
			expectedIdx: 1,
			expectedKey: "missing",
		},
		{
			name: "incompatible source key",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("conflict"), Src: tx.Bucket([]byte("z")), Dst: tx.Bucket([]byte("y"))},
				}
			},
			expectedErr: berrors.ErrIncompatibleValue,
			expectedIdx: 1,
			expectedKey: "conflict",
		},
		{
			name: "destination already contains the target bucket",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				x := tx.Bucket([]byte("x"))
				if x.Bucket([]byte("b")) == nil {
					if _, err := x.CreateBucket([]byte("b")); err != nil {
						t.Fatal(err)
					}
				}
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: x},
				}
			},
			expectedErr: berrors.ErrBucketExists,
			expectedIdx: 0,
			expectedKey: "b",
		},
		{
			name: "same source bucket moved twice",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
				}
			},
			expectedErr: berrors.ErrBucketNotFound,
			expectedIdx: 1,
			expectedKey: "b",
		},
		{
			name: "incompatible destination key",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				// z already holds a plain key "conflict"; first make "conflict"
				// a real sub-bucket of y so the source lookup succeeds.
				y := tx.Bucket([]byte("y"))
				if y.Bucket([]byte("conflict")) == nil {
					if _, err := y.CreateBucket([]byte("conflict")); err != nil {
						t.Fatal(err)
					}
				}
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("conflict"), Src: y, Dst: tx.Bucket([]byte("z"))},
				}
			},
			expectedErr: berrors.ErrIncompatibleValue,
			expectedIdx: 1,
			expectedKey: "conflict",
		},
		{
			name: "same source and destination bucket",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				a := tx.Bucket([]byte("a"))
				return []bbolt.MoveBucketSpec{
					{Key: []byte("y"), Src: nil, Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("b"), Src: a, Dst: a},
				}
			},
			expectedErr: berrors.ErrSameBuckets,
			expectedIdx: 1,
			expectedKey: "b",
		},
		{
			name: "second source bucket does not exist",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("b2"), Src: nil, Dst: tx.Bucket([]byte("x"))},
				}
			},
			expectedErr: berrors.ErrBucketNotFound,
			expectedIdx: 1,
			expectedKey: "b2",
		},
		{
			name: "cyclic move into own descendant",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				a := tx.Bucket([]byte("a"))
				deep := a.Bucket([]byte("b")).Bucket([]byte("deep"))
				return []bbolt.MoveBucketSpec{
					{Key: []byte("y"), Src: nil, Dst: tx.Bucket([]byte("x"))},
					{Key: []byte("b"), Src: a, Dst: deep},
				}
			},
			expectedErr: berrors.ErrCyclicBucketMove,
			expectedIdx: 1,
			expectedKey: "b",
		},
		{
			name: "cyclic move into self",
			buildSpecs: func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
				a := tx.Bucket([]byte("a"))
				b := a.Bucket([]byte("b"))
				return []bbolt.MoveBucketSpec{
					{Key: []byte("b"), Src: a, Dst: b},
				}
			},
			expectedErr: berrors.ErrCyclicBucketMove,
			expectedIdx: 0,
			expectedKey: "b",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupBatchMoveDB(t)
			before := dumpDB(t, db)

			err := db.Update(func(tx *bbolt.Tx) error {
				return tx.MoveBuckets(tc.buildSpecs(tx))
			})
			require.Error(t, err)
			var bme *bbolt.BucketMoveError
			require.True(t, errors.As(err, &bme), "expected *BucketMoveError, got %v", err)
			require.ErrorIs(t, err, tc.expectedErr)
			require.Equal(t, tc.expectedIdx, bme.Index)
			require.Equal(t, tc.expectedKey, string(bme.Key))

			// The whole batch must be rolled back at the structure level:
			// nothing moved even though the first spec was valid.
			after := dumpDB(t, db)
			require.Equal(t, before, after, "failed batch must not modify any bucket")
		})
	}
}

func TestTx_MoveBuckets_DuplicateDestinationSlot(t *testing.T) {
	db := setupBatchMoveDB(t)

	// Both moves land in the same (dst,key) slot: x/w and x/w, sourced from
	// two different buckets that each contain a child named "w".
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		x := tx.Bucket([]byte("x"))
		y := tx.Bucket([]byte("y"))
		if _, err := x.CreateBucket([]byte("w")); err != nil {
			return err
		}
		_, err := y.CreateBucket([]byte("w"))
		return err
	}))
	before := dumpDB(t, db)

	err := db.Update(func(tx *bbolt.Tx) error {
		return tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("w"), Src: tx.Bucket([]byte("x")), Dst: tx.Bucket([]byte("a"))},
			{Key: []byte("w"), Src: tx.Bucket([]byte("y")), Dst: tx.Bucket([]byte("a"))},
		})
	})
	require.Error(t, err)
	require.ErrorIs(t, err, berrors.ErrBucketExists)
	var bme *bbolt.BucketMoveError
	require.True(t, errors.As(err, &bme))
	require.Equal(t, 1, bme.Index)
	require.Equal(t, "w", string(bme.Key))

	// Nothing moved.
	require.Equal(t, before, dumpDB(t, db))
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte("x")).Bucket([]byte("w")))
		require.NotNil(t, tx.Bucket([]byte("y")).Bucket([]byte("w")))
		require.Nil(t, tx.Bucket([]byte("a")).Bucket([]byte("w")))
		return nil
	}))
}

func TestTx_MoveBuckets_DifferentDB(t *testing.T) {
	db1 := btesting.MustCreateDB(t)
	db2 := btesting.MustCreateDB(t)
	defer func() { _ = db1.Close(); _ = db2.Close() }()

	// Open a concurrent, still-open write transaction on the second database
	// so its bucket is live but belongs to a different DB/Tx.
	foreignTx, err := db2.Begin(true)
	require.NoError(t, err)
	defer func() { _ = foreignTx.Rollback() }()
	foreignBucket, err := foreignTx.CreateBucket([]byte("foreignDst"))
	require.NoError(t, err)

	err = db1.Update(func(tx *bbolt.Tx) error {
		src, err := tx.CreateBucket([]byte("src"))
		if err != nil {
			return err
		}
		if _, err := src.CreateBucket([]byte("child")); err != nil {
			return err
		}
		return tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("child"), Src: src, Dst: foreignBucket},
		})
	})
	require.Error(t, err)
	require.ErrorIs(t, err, berrors.ErrDifferentDB)
	var bme *bbolt.BucketMoveError
	require.True(t, errors.As(err, &bme))
	require.Equal(t, 0, bme.Index)
}

// TestTx_MoveBuckets_SlotReleasedBySiblingMove verifies a destination slot
// occupied by a bucket that another spec in the same batch moves away is
// reusable, regardless of spec ordering.
func TestTx_MoveBuckets_SlotReleasedBySiblingMove(t *testing.T) {
	db := setupBatchMoveDB(t)

	// index 0 frees the root slot "y" by moving it under x;
	// index 1 reuses the freed root slot to move a/b to root as "y".
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("y"), Src: nil, Dst: tx.Bucket([]byte("x"))},
			{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: nil},
		})
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		// a/b moved to root under key "b"; root/y moved under x.
		require.NotNil(t, tx.Bucket([]byte("b")))
		require.Nil(t, tx.Bucket([]byte("a")).Bucket([]byte("b")))
		require.NotNil(t, tx.Bucket([]byte("x")).Bucket([]byte("y")))
		require.Nil(t, tx.Bucket([]byte("y")))
		return nil
	}))

	// Reverse the batch ordering in a fresh tx to prove order independence.
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: nil, Dst: tx.Bucket([]byte("a"))},
			{Key: []byte("y"), Src: tx.Bucket([]byte("x")), Dst: nil},
		})
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte("y")))
		require.NotNil(t, tx.Bucket([]byte("a")).Bucket([]byte("b")))
		require.Nil(t, tx.Bucket([]byte("x")).Bucket([]byte("y")))
		require.Nil(t, tx.Bucket([]byte("b")))
		return nil
	}))
}

// TestTx_MoveBuckets_CrossSpecCycle verifies cycles formed only by the
// combination of otherwise-valid moves are detected and rejected.
func TestTx_MoveBuckets_CrossSpecCycle(t *testing.T) {
	db := setupBatchMoveDB(t)
	before := dumpDB(t, db)

	err := db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		b := a.Bucket([]byte("b"))
		x := tx.Bucket([]byte("x"))
		return tx.MoveBuckets([]bbolt.MoveBucketSpec{
			// Move x underneath b ...
			{Key: []byte("x"), Src: nil, Dst: b},
			// ... and then move b underneath x: would create b -> x -> b.
			{Key: []byte("b"), Src: a, Dst: x},
		})
	})
	require.Error(t, err)
	require.ErrorIs(t, err, berrors.ErrCyclicBucketMove)
	var bme *bbolt.BucketMoveError
	require.True(t, errors.As(err, &bme))
	require.Equal(t, 1, bme.Index)
	require.Equal(t, before, dumpDB(t, db))
}

// TestTx_MoveBuckets_PreservesPointersSequenceAndCache verifies the ownership
// guarantees: an already-opened child bucket and its descendant keep working
// through the same pointer after a batch move, sequence is preserved, and
// writes through the old pointer land in the relocated bucket.
func TestTx_MoveBuckets_PreservesPointersSequenceAndCache(t *testing.T) {
	db := setupBatchMoveDB(t)

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		x := tx.Bucket([]byte("x"))
		b := a.Bucket([]byte("b"))
		deep := b.Bucket([]byte("deep"))

		if err := tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: a, Dst: x},
		}); err != nil {
			return err
		}

		// The cached pointers must be the same instances now owned by x.
		require.Same(t, b, x.Bucket([]byte("b")))
		require.Same(t, deep, x.Bucket([]byte("b")).Bucket([]byte("deep")))
		require.Equal(t, uint64(7), b.Sequence())

		// A write through the stale pointer must persist into the new home.
		return b.Put([]byte("late"), []byte("write"))
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("x")).Bucket([]byte("b"))
		require.Equal(t, uint64(7), b.Sequence())
		require.Equal(t, []byte("v1"), b.Get([]byte("k1")))
		require.Equal(t, []byte("write"), b.Get([]byte("late")))
		require.Equal(t, []byte("dv"), b.Bucket([]byte("deep")).Get([]byte("dk")))
		return nil
	}))
}

// TestTx_MoveBuckets_RollbackRestoresOriginalPosition verifies that when the
// surrounding transaction rolls back, every relocated bucket returns to its
// original position.
func TestTx_MoveBuckets_RollbackRestoresOriginalPosition(t *testing.T) {
	db := setupBatchMoveDB(t)
	before := dumpDB(t, db)

	require.Error(t, db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		x := tx.Bucket([]byte("x"))
		if err := tx.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: a, Dst: x},
			{Key: []byte("y"), Src: nil, Dst: x},
		}); err != nil {
			return err
		}
		// Sanity: within the tx the move is visible.
		if x.Bucket([]byte("b")) == nil {
			t.Fatal("move not visible inside transaction")
		}
		return errors.New("abort the transaction")
	}))

	require.Equal(t, before, dumpDB(t, db))
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		require.NotNil(t, tx.Bucket([]byte("a")).Bucket([]byte("b")))
		require.NotNil(t, tx.Bucket([]byte("y")))
		require.Nil(t, tx.Bucket([]byte("x")).Bucket([]byte("b")))
		return nil
	}))
}

// TestTx_MoveBuckets_CancelRestoresOriginalPosition verifies an explicit
// caller cancellation (rolling back a manually managed transaction) restores
// every bucket to its original position.
func TestTx_MoveBuckets_CancelRestoresOriginalPosition(t *testing.T) {
	db := setupBatchMoveDB(t)
	before := dumpDB(t, db)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	require.NoError(t, tx.MoveBuckets([]bbolt.MoveBucketSpec{
		{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
		{Key: []byte("y"), Src: nil, Dst: tx.Bucket([]byte("x"))},
	}))
	// Visible inside the open transaction...
	require.NotNil(t, tx.Bucket([]byte("x")).Bucket([]byte("b")))

	// ...then the caller cancels.
	require.NoError(t, tx.Rollback())

	require.Equal(t, before, dumpDB(t, db))
}

// TestTx_MoveBuckets_IsReplayable verifies a batch can be re-attempted after a
// failure without creating duplicate buckets: the failed attempt changes
// nothing, and the corrected attempt succeeds exactly once.
func TestTx_MoveBuckets_IsReplayable(t *testing.T) {
	db := setupBatchMoveDB(t)

	specs := func(tx *bbolt.Tx) []bbolt.MoveBucketSpec {
		return []bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
			{Key: []byte("y"), Src: nil, Dst: tx.Bucket([]byte("x"))},
		}
	}

	// First attempt with a bad extra spec fails atomically.
	require.Error(t, db.Update(func(tx *bbolt.Tx) error {
		bad := append(specs(tx), bbolt.MoveBucketSpec{
			Key: []byte("nope"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("y")),
		})
		return tx.MoveBuckets(bad)
	}))

	// Replaying the correct batch in a fresh transaction succeeds once.
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		return tx.MoveBuckets(specs(tx))
	}))

	// Replaying it again must fail (sources are gone) and must not duplicate.
	err := db.Update(func(tx *bbolt.Tx) error {
		return tx.MoveBuckets(specs(tx))
	})
	require.Error(t, err)
	require.ErrorIs(t, err, berrors.ErrBucketNotFound)

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		x := tx.Bucket([]byte("x"))
		// Exactly one b and one y under x.
		count := 0
		require.NoError(t, x.ForEachBucket(func(k []byte) error {
			count++
			return nil
		}))
		require.Equal(t, 2, count)
		return nil
	}))
}

// TestTx_MoveBuckets_DeepNestingAndMultipleMoves exercises a larger batch
// across deeply nested buckets, including top level moves, and verifies every
// relocated subtree keeps its full contents.
func TestTx_MoveBuckets_DeepNestingAndMultipleMoves(t *testing.T) {
	db := setupBatchMoveDB(t)

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		b := a.Bucket([]byte("b"))
		x := tx.Bucket([]byte("x"))
		y := tx.Bucket([]byte("y"))

		return tx.MoveBuckets([]bbolt.MoveBucketSpec{
			// Nested bucket relocates under a sibling.
			{Key: []byte("deep"), Src: b, Dst: y},
			// Parent b relocates under x.
			{Key: []byte("b"), Src: a, Dst: x},
			// Top level bucket becomes nested.
			{Key: []byte("z"), Src: nil, Dst: x},
		})
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		x := tx.Bucket([]byte("x"))
		y := tx.Bucket([]byte("y"))

		// b moved under x and kept its sequence and data.
		b := x.Bucket([]byte("b"))
		require.NotNil(t, b)
		require.Equal(t, uint64(7), b.Sequence())
		require.Equal(t, []byte("v1"), b.Get([]byte("k1")))

		// deep moved under y and kept its own data.
		deep := y.Bucket([]byte("deep"))
		require.NotNil(t, deep)
		require.Equal(t, []byte("dv"), deep.Get([]byte("dk")))
		// b no longer contains deep.
		require.Nil(t, b.Bucket([]byte("deep")))

		// z moved under x, plain key intact.
		z := x.Bucket([]byte("z"))
		require.NotNil(t, z)
		require.Equal(t, []byte("plain"), z.Get([]byte("conflict")))
		require.Nil(t, tx.Bucket([]byte("z")))
		return nil
	}))
}

func TestBucket_MoveBuckets_DelegatesToTx(t *testing.T) {
	db := setupBatchMoveDB(t)

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		x := tx.Bucket([]byte("x"))
		return a.MoveBuckets([]bbolt.MoveBucketSpec{
			{Key: []byte("b"), Src: a, Dst: x},
		})
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte("a")).Bucket([]byte("b")))
		require.NotNil(t, tx.Bucket([]byte("x")).Bucket([]byte("b")))
		return nil
	}))
}

// TestTx_MoveBuckets_SingleMoveParity verifies the batch and single entry
// points produce structurally identical databases for an equivalent move.
func TestTx_MoveBuckets_SingleMoveParity(t *testing.T) {
	run := func(t *testing.T, useBatch bool) string {
		db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})
		require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
			src, err := tx.CreateBucket([]byte("src"))
			if err != nil {
				return err
			}
			child, err := src.CreateBucket([]byte("child"))
			if err != nil {
				return err
			}
			if err := child.SetSequence(99); err != nil {
				return err
			}
			return child.Put([]byte("k"), []byte("v"))
		}))
		require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
			dst, err := tx.CreateBucket([]byte("dst"))
			if err != nil {
				return err
			}
			src := tx.Bucket([]byte("src"))
			if useBatch {
				return tx.MoveBuckets([]bbolt.MoveBucketSpec{
					{Key: []byte("child"), Src: src, Dst: dst},
				})
			}
			return tx.MoveBucket([]byte("child"), src, dst)
		}))
		dump := dumpDB(t, db)
		require.NoError(t, db.Close())
		return dump
	}

	single := run(t, false)
	batch := run(t, true)
	require.Equal(t, single, batch)
}

// dumpDB renders the whole bucket tree: every bucket's path, sequence, plain
// key/value pairs and nested buckets. It is stable across transactions that
// make no logical change (unlike a raw byte dump, which also captures the
// meta transaction id and freelist layout).
func dumpDB(t *testing.T, db *btesting.DB) string {
	t.Helper()
	var out strings.Builder
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			dumpBucketStructure([]string{string(name)}, b, &out)
			return nil
		})
	}))
	return out.String()
}

func dumpBucketStructure(path []string, b *bbolt.Bucket, out *strings.Builder) {
	if b == nil {
		return
	}
	name := "/" + strings.Join(path, "/")
	fmt.Fprintf(out, "bucket %s seq=%d\n", name, b.Sequence())

	// Plain key/value pairs first; nested buckets return a nil value and are
	// rendered by the recursion below.
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil {
			continue
		}
		fmt.Fprintf(out, "  kv %q=%q\n", string(k), string(v))
	}

	// Recurse into nested buckets in deterministic key order.
	_ = b.ForEachBucket(func(k []byte) error {
		dumpBucketStructure(append(append([]string{}, path...), string(k)), b.Bucket(k), out)
		return nil
	})
}
