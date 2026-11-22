package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// bigEndianKey encodes n as a fixed-width, sortable byte slice.
func bigEndianKey(n uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, n)
	return k
}

// seedPrefixBucket inserts keys prefix/0..count-1 (plus a few keys outside the
// prefix). Callers must commit afterwards and use requireMultiLeafPages in a
// later transaction to prove the bucket spans multiple leaf pages, since page
// statistics are only populated after nodes spill on commit.
func seedPrefixBucket(t *testing.T, b *bolt.Bucket, prefix []byte, count int, valueSize int) {
	t.Helper()
	for i := 0; i < count; i++ {
		k := append(append([]byte{}, prefix...), bigEndianKey(uint64(i))...)
		require.NoError(t, b.Put(k, make([]byte, valueSize)))
	}
	// Keys outside the prefix, on both sides of the range.
	require.NoError(t, b.Put([]byte("o/left-neighbor"), []byte("left-neighbor")))
	require.NoError(t, b.Put([]byte("zzz-outside-prefix"), []byte("right-neighbor")))
}

// requireMultiLeafPages asserts the bucket is backed by more than one leaf
// page, so traversal tests genuinely exercise page boundaries. It must run in
// a transaction opened after the seeding transaction committed.
func requireMultiLeafPages(t *testing.T, b *bolt.Bucket) {
	t.Helper()
	require.Greater(t, b.Stats().LeafPageN, 1, "test bucket must span multiple leaf pages")
}

// setupMultiPageBucket seeds a bucket that spans multiple leaf pages and
// returns once the seeding transaction has committed.
func setupMultiPageBucket(t *testing.T, db *btesting.DB, bucket, prefix []byte, count int) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		require.NoError(t, err)
		seedPrefixBucket(t, b, prefix, count, 100)
		return nil
	}))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		requireMultiLeafPages(t, tx.Bucket(bucket))
		return nil
	}))
}

// TestPrefixCursor_Constructor verifies creation-time validation.
func TestPrefixCursor_Constructor(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)

		// Deletion requires a writable transaction but the flag is fine here.
		pc, err := b.PrefixCursor([]byte("p"), bolt.Forward, true)
		require.NoError(t, err)
		require.Equal(t, []byte("p"), pc.Prefix())
		require.Same(t, b, pc.Bucket())
		pc.Close()

		// Invalid direction.
		_, err = b.PrefixCursor([]byte("p"), 42, false)
		require.ErrorIs(t, err, berrors.ErrInvalidDirection)
		return nil
	}))

	// Read-only transaction rejects the delete flag.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		_, err := b.PrefixCursor([]byte("p"), bolt.Forward, true)
		require.ErrorIs(t, err, berrors.ErrPrefixDeleteNotAllowed)

		// Read-only iteration without deletion is allowed.
		pc, err := b.PrefixCursor([]byte("p"), bolt.Forward, false)
		require.NoError(t, err)
		require.ErrorIs(t, pc.Delete(), berrors.ErrPrefixDeleteNotAllowed)
		return nil
	}))
}

// TestPrefixCursor_EmptyBucket verifies consistent results on an empty bucket
// for every positioning method and both directions.
func TestPrefixCursor_EmptyBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	for _, direction := range []int{bolt.Forward, bolt.Reverse} {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("empty"))
			require.NoError(t, err)

			pc, err := b.PrefixCursor([]byte("p/"), direction, true)
			require.NoError(t, err)

			k, _, err := pc.First()
			require.NoError(t, err)
			require.Nil(t, k)

			k, _, err = pc.Last()
			require.NoError(t, err)
			require.Nil(t, k)

			k, _, err = pc.Seek([]byte("p/0"))
			require.NoError(t, err)
			require.Nil(t, k)

			// Next/Prev before and after exhausting the range stay empty.
			k, _, err = pc.Next()
			require.NoError(t, err)
			require.Nil(t, k)
			k, _, err = pc.Prev()
			require.NoError(t, err)
			require.Nil(t, k)

			require.ErrorIs(t, pc.Delete(), berrors.ErrCursorNotPositioned)
			return nil
		}))
	}
}

// TestPrefixCursor_ForwardOrder verifies plain forward iteration matches the
// full lexicographical order of the prefixed keys.
func TestPrefixCursor_ForwardOrder(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 500
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, false)
		require.NoError(t, err)

		var got [][]byte
		for k, _, err := pc.First(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			got = append(got, append([]byte{}, k...))
		}

		want := expectedPrefixedKeys(prefix, count)
		require.Equal(t, want, got)
		require.Len(t, got, count)
		return nil
	}))
}

// TestPrefixCursor_ReverseOrder verifies reverse iteration returns exactly the
// prefixed keys in descending order.
func TestPrefixCursor_ReverseOrder(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 500
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Reverse, false)
		require.NoError(t, err)

		var got [][]byte
		for k, _, err := pc.Last(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			got = append(got, append([]byte{}, k...))
		}

		want := expectedPrefixedKeys(prefix, count)
		sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i], want[j]) > 0 })
		require.Equal(t, want, got)
		return nil
	}))
}

// TestPrefixCursor_DeleteForward is the core batch-cleanup scenario: iterate
// and delete every prefixed key in forward direction across page boundaries
// and prove no key is skipped or revisited.
func TestPrefixCursor_DeleteForward(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 500
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	var deleted [][]byte
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		seen := make(map[string]bool)
		for k, _, err := pc.First(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			require.True(t, bytes.HasPrefix(k, prefix))
			require.False(t, seen[string(k)], "key revisited: %x", k)
			seen[string(k)] = true
			deleted = append(deleted, append([]byte{}, k...))
			require.NoError(t, pc.Delete())
		}
		require.Len(t, seen, count)
		return nil
	}))

	require.Equal(t, expectedPrefixedKeys(prefix, count), deleted)
	requireAllPrefixedRemoved(t, db, "data", prefix, count)
}

// TestPrefixCursor_DeleteReverse exercises the same guarantee while walking
// backwards, including the delete-then-continue path across page boundaries.
func TestPrefixCursor_DeleteReverse(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 500
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	var deleted [][]byte
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Reverse, true)
		require.NoError(t, err)

		seen := make(map[string]bool)
		for k, _, err := pc.Last(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			require.False(t, seen[string(k)], "key revisited: %x", k)
			seen[string(k)] = true
			deleted = append(deleted, append([]byte{}, k...))
			require.NoError(t, pc.Delete())
		}
		require.Len(t, seen, count)
		return nil
	}))

	want := expectedPrefixedKeys(prefix, count)
	sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i], want[j]) > 0 })
	require.Equal(t, want, deleted)
	requireAllPrefixedRemoved(t, db, "data", prefix, count)
}

// TestPrefixCursor_DeleteEveryOtherKey ensures interleaved deletions never
// disturb neighbors: keys that survive must still all be visited exactly once.
func TestPrefixCursor_DeleteEveryOtherKey(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 400
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		visited := 0
		i := 0
		for k, _, err := pc.First(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			visited++
			if i%2 == 0 {
				require.NoError(t, pc.Delete())
			}
			i++
		}
		require.Equal(t, count, visited)
		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		c := b.Cursor()
		survivors := 0
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			survivors++
		}
		require.Equal(t, count/2, survivors)
		// Neighbor keys outside the prefix are untouched.
		require.Equal(t, []byte("right-neighbor"), b.Get([]byte("zzz-outside-prefix")))
		return nil
	}))
}

// TestPrefixCursor_Seek covers forward and reverse seek semantics, including
// seeks that do not exactly match a key and seeks outside the prefix range.
func TestPrefixCursor_Seek(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 300
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))

		// Forward seek to an exact key.
		pc, err := b.PrefixCursor(prefix, bolt.Forward, false)
		require.NoError(t, err)
		k, _, err := pc.Seek(prefixedKey(prefix, 10))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 10), k)
		k, _, err = pc.Next()
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 11), k)

		// Forward seek below the prefix lands on the first prefixed key.
		k, _, err = pc.Seek([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 0), k)

		// Forward seek past the prefix yields nil.
		k, _, err = pc.Seek([]byte("zzz"))
		require.NoError(t, err)
		require.Nil(t, k)

		// Reverse seek to an exact key.
		rpc, err := b.PrefixCursor(prefix, bolt.Reverse, false)
		require.NoError(t, err)
		k, _, err = rpc.Seek(prefixedKey(prefix, 10))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 10), k)
		k, _, err = rpc.Next()
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 9), k)

		// Reverse seek past the range returns the last prefixed key.
		k, _, err = rpc.Seek([]byte("zzz"))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, count-1), k)

		// Reverse seek below the prefix yields nil.
		k, _, err = rpc.Seek([]byte("a"))
		require.NoError(t, err)
		require.Nil(t, k)
		return nil
	}))
}

// TestPrefixCursor_SeekAfterDelete proves repositioning with Seek after
// deletions gives the same ordered result regardless of node rearrangement.
func TestPrefixCursor_SeekAfterDelete(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	const count = 300
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		// Delete the first 100 keys via stable traversal.
		k, _, err := pc.First()
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			require.NotNil(t, k)
			require.NoError(t, pc.Delete())
			k, _, err = pc.Next()
			require.NoError(t, err)
		}

		// Seek onto a remaining key and finish the cleanup.
		k, _, err = pc.Seek(prefixedKey(prefix, 200))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 200), k)
		visited := 0
		for ; k != nil; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			visited++
			require.NoError(t, pc.Delete())
		}
		require.Equal(t, 100, visited) // keys 200..299
		return nil
	}))

	// Keys 100..199 were intentionally left in place and must survive.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		c := b.Cursor()
		survivors := 0
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			require.Equal(t, prefixedKey(prefix, 100+survivors), k)
			survivors++
		}
		require.Equal(t, 100, survivors)
		return nil
	}))
}

// TestPrefixCursor_NestedBucket verifies nested buckets appear with nil value,
// cannot be deleted through the cursor, and do not break traversal of the
// ordinary keys around them.
func TestPrefixCursor_NestedBucket(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		require.NoError(t, err)
		seedPrefixBucket(t, b, prefix, 100, 100)
		_, err = b.CreateBucket(append(append([]byte{}, prefix...), []byte("bucket")...))
		require.NoError(t, err)
		return nil
	}))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		requireMultiLeafPages(t, tx.Bucket([]byte("data")))
		return nil
	}))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		var keys [][]byte
		var sawNested bool
		for k, v, err := pc.First(); ; k, v, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			keys = append(keys, append([]byte{}, k...))
			if string(k) == string(prefix)+"bucket" {
				sawNested = true
				require.Nil(t, v)
				require.ErrorIs(t, pc.Delete(), berrors.ErrIncompatibleValue)
			}
		}
		require.True(t, sawNested)
		// Every prefixed key, including the nested bucket, was visited once.
		require.Len(t, keys, 101)
		return nil
	}))
}

// TestPrefixCursor_ReturnedKeyIsCopy verifies the returned key remains valid
// after the cursor advances and after the key itself is deleted.
func TestPrefixCursor_ReturnedKeyIsCopy(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")
	const count = 200

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		first, _, err := pc.First()
		require.NoError(t, err)
		firstCopy := append([]byte{}, first...)
		require.NoError(t, pc.Delete())

		second, _, err := pc.Next()
		require.NoError(t, err)
		require.Equal(t, firstCopy, prefixedKey(prefix, 0))
		require.Equal(t, second, prefixedKey(prefix, 1))
		return nil
	}))
}

// TestPrefixCursor_RepeatedCleanup verifies idempotency: running the same
// prefix cleanup a second time visits nothing, changes nothing, and deleting
// an already-deleted current item never alters subsequent results.
func TestPrefixCursor_RepeatedCleanup(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")
	const count = 300

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	cleanup := func(expectDeletions int) {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("data"))
			pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
			require.NoError(t, err)

			deletions := 0
			for k, _, err := pc.First(); ; k, _, err = pc.Next() {
				require.NoError(t, err)
				if k == nil {
					break
				}
				require.NoError(t, pc.Delete())
				deletions++
				// The item is already gone: repeating the delete reports the
				// not-positioned state and must not change traversal state.
				require.ErrorIs(t, pc.Delete(), berrors.ErrCursorNotPositioned)
			}
			require.Equal(t, expectDeletions, deletions)
			return nil
		}))
	}

	cleanup(count)
	requireAllPrefixedRemoved(t, db, "data", prefix, count)
	// Re-running the cleanup is a stable no-op.
	cleanup(0)
	requireAllPrefixedRemoved(t, db, "data", prefix, count)
}

// TestPrefixCursor_ResumeInNewTx verifies recoverability: after a transaction
// ends with part of the prefix removed, a new cursor resumes from the first
// remaining key and finishes the work without revisiting deleted keys.
func TestPrefixCursor_ResumeInNewTx(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")
	const count = 300

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	// First transaction removes keys 0..99, then commits.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)
		k, _, err := pc.First()
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			require.NotNil(t, k)
			require.NoError(t, pc.Delete())
			k, _, err = pc.Next()
			require.NoError(t, err)
		}
		return nil
	}))

	// Second transaction resumes from a persisted position marker and proves
	// only the remaining keys are visited.
	marker := prefixedKey(prefix, 100)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		visited := 0
		for k, _, err := pc.Seek(marker); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			require.Equal(t, prefixedKey(prefix, 100+visited), k)
			require.NoError(t, pc.Delete())
			visited++
		}
		require.Equal(t, 200, visited)
		return nil
	}))

	requireAllPrefixedRemoved(t, db, "data", prefix, count)
}

// TestPrefixCursor_TxClosed verifies that closing the transaction puts the
// cursor in a recognizable terminal state and that no method afterwards can
// reach the old page references.
func TestPrefixCursor_TxClosed(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")

	setupMultiPageBucket(t, db, []byte("data"), prefix, 100)

	var pc *bolt.PrefixCursor
	tx, err := db.Begin(true)
	require.NoError(t, err)
	pc, err = tx.Bucket([]byte("data")).PrefixCursor(prefix, bolt.Forward, true)
	require.NoError(t, err)

	k, _, err := pc.First()
	require.NoError(t, err)
	require.NotNil(t, k)

	require.NoError(t, tx.Rollback())

	// Every entry point now returns the terminal error and keeps returning it.
	_, _, err = pc.Next()
	require.ErrorIs(t, err, berrors.ErrCursorTerminal)
	_, _, err = pc.First()
	require.ErrorIs(t, err, berrors.ErrCursorTerminal)
	_, _, err = pc.Last()
	require.ErrorIs(t, err, berrors.ErrCursorTerminal)
	_, _, err = pc.Seek(prefixedKey(prefix, 1))
	require.ErrorIs(t, err, berrors.ErrCursorTerminal)
	_, _, err = pc.Prev()
	require.ErrorIs(t, err, berrors.ErrCursorTerminal)
	require.ErrorIs(t, pc.Delete(), berrors.ErrCursorTerminal)

	// Explicit close is idempotent and yields the same terminal state.
	pc.Close()
	_, _, err = pc.Next()
	require.ErrorIs(t, err, berrors.ErrCursorTerminal)
}

// TestPrefixCursor_BucketDeleted verifies that deleting the iterated bucket
// through its parent makes every later cursor call fail with the terminal
// error instead of touching freed pages.
func TestPrefixCursor_BucketDeleted(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		parent, err := tx.CreateBucketIfNotExists([]byte("parent"))
		require.NoError(t, err)
		child, err := parent.CreateBucketIfNotExists([]byte("child"))
		require.NoError(t, err)
		seedPrefixBucket(t, child, prefix, 100, 100)
		return nil
	}))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		requireMultiLeafPages(t, tx.Bucket([]byte("parent")).Bucket([]byte("child")))
		return nil
	}))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		parent := tx.Bucket([]byte("parent"))
		child := parent.Bucket([]byte("child"))
		pc, err := child.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		k, _, err := pc.First()
		require.NoError(t, err)
		require.NotNil(t, k)

		require.NoError(t, parent.DeleteBucket([]byte("child")))

		_, _, err = pc.Next()
		require.ErrorIs(t, err, berrors.ErrCursorTerminal)
		require.ErrorIs(t, pc.Delete(), berrors.ErrCursorTerminal)
		_, _, err = pc.Seek(prefixedKey(prefix, 5))
		require.ErrorIs(t, err, berrors.ErrCursorTerminal)
		return nil
	}))
}

// TestPrefixCursor_CloseIsTerminal verifies Close detaches page references and
// flips the cursor into the sticky terminal state.
func TestPrefixCursor_CloseIsTerminal(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("p/1"), []byte("v")))

		pc, err := b.PrefixCursor([]byte("p/"), bolt.Forward, true)
		require.NoError(t, err)
		k, _, err := pc.First()
		require.NoError(t, err)
		require.Equal(t, []byte("p/1"), k)

		pc.Close()
		pc.Close() // idempotent
		_, _, err = pc.Next()
		require.ErrorIs(t, err, berrors.ErrCursorTerminal)
		return nil
	}))
}

// TestPrefixCursor_NextBeforePosition verifies Next/Prev before any
// positioning call behave like First/Last according to direction.
func TestPrefixCursor_NextBeforePosition(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")
	const count = 100

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))

		fwd, err := b.PrefixCursor(prefix, bolt.Forward, false)
		require.NoError(t, err)
		k, _, err := fwd.Next()
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 0), k)

		rev, err := b.PrefixCursor(prefix, bolt.Reverse, false)
		require.NoError(t, err)
		k, _, err = rev.Next()
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, count-1), k)

		// Prev before positioning starts from the opposite boundary.
		fresh, err := b.PrefixCursor(prefix, bolt.Forward, false)
		require.NoError(t, err)
		k, _, err = fresh.Prev()
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, count-1), k)
		return nil
	}))
}

// TestPrefixCursor_PlainCursorBehaviorUnchanged sanity-checks that the
// pre-existing Cursor still follows its original semantics (this guards the
// shared internal cursor machinery against accidental behavior changes).
func TestPrefixCursor_PlainCursorBehaviorUnchanged(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("k1"), []byte("v1")))
		require.NoError(t, b.Put([]byte("k2"), []byte("v2")))

		c := b.Cursor()
		var keys [][]byte
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, append([]byte{}, k...))
		}
		require.Equal(t, [][]byte{[]byte("k1"), []byte("k2")}, keys)

		// First on an empty bucket still returns nil,nil.
		empty, err := tx.CreateBucketIfNotExists([]byte("empty"))
		require.NoError(t, err)
		ec := empty.Cursor()
		k, v := ec.First()
		require.Nil(t, k)
		require.Nil(t, v)
		k, v = ec.Last()
		require.Nil(t, k)
		require.Nil(t, v)
		return nil
	}))
}

// TestPrefixCursor_PrefixBoundary verifies prefix scoping is exact: keys that
// merely share a leading substring but not the full prefix are excluded on
// both ends and in both directions.
func TestPrefixCursor_PrefixBoundary(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("p"), []byte("short")))
		require.NoError(t, b.Put([]byte("p1"), []byte("no-slash")))
		require.NoError(t, b.Put([]byte("p/1"), []byte("a")))
		require.NoError(t, b.Put([]byte("p/2"), []byte("b")))
		require.NoError(t, b.Put([]byte("p0"), []byte("no-slash-2")))
		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))

		fwd, err := b.PrefixCursor([]byte("p/"), bolt.Forward, false)
		require.NoError(t, err)
		var got [][]byte
		for k, _, err := fwd.First(); ; k, _, err = fwd.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			got = append(got, append([]byte{}, k...))
		}
		require.Equal(t, [][]byte{[]byte("p/1"), []byte("p/2")}, got)

		rev, err := b.PrefixCursor([]byte("p/"), bolt.Reverse, false)
		require.NoError(t, err)
		got = nil
		for k, _, err := rev.Last(); ; k, _, err = rev.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			got = append(got, append([]byte{}, k...))
		}
		require.Equal(t, [][]byte{[]byte("p/2"), []byte("p/1")}, got)
		return nil
	}))
}

// TestPrefixCursor_AllFFPrefix exercises the edge case where the prefix is
// made of 0xff bytes and no strict successor key exists.
func TestPrefixCursor_AllFFPrefix(t *testing.T) {
	db := btesting.MustCreateDB(t)
	prefix := []byte{0xff, 0xff}

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte{0xff, 0xfe}, []byte("before")))
		require.NoError(t, b.Put([]byte{0xff, 0xff, 0x00}, []byte("match1")))
		require.NoError(t, b.Put([]byte{0xff, 0xff, 0x01}, []byte("match2")))
		return nil
	}))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		pc, err := b.PrefixCursor(prefix, bolt.Reverse, true)
		require.NoError(t, err)

		k, _, err := pc.Last()
		require.NoError(t, err)
		require.Equal(t, []byte{0xff, 0xff, 0x01}, k)

		k, _, err = pc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte{0xff, 0xff, 0x00}, k)
		require.NoError(t, pc.Delete())

		k, _, err = pc.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		return nil
	}))
}

// TestPrefixCursor_QuickCheck runs randomized mixed forward/delete workloads
// and cross-checks the surviving key set against an independent model.
func TestPrefixCursor_QuickCheck(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("q/")
	const count = 400

	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	// Model: decide upfront which keys survive.
	survive := make(map[string]bool)
	for i := 0; i < count; i++ {
		survive[string(prefixedKey(prefix, i))] = i%3 != 0 // delete every third key
	}

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		visited := make(map[string]bool)
		for k, _, err := pc.First(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			require.False(t, visited[string(k)], "revisited %x", k)
			visited[string(k)] = true
			if !survive[string(k)] {
				require.NoError(t, pc.Delete())
			}
		}
		require.Len(t, visited, count)
		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		c := b.Cursor()
		got := make(map[string]bool)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			got[string(k)] = true
		}
		// The model records both outcomes; only the true entries correspond to
		// keys that must still exist.
		want := make(map[string]bool)
		for k, keeps := range survive {
			if keeps {
				want[k] = true
			}
		}
		require.Equal(t, want, got)

		// Keys outside the prefix are outside the cursor's scope and survive.
		require.Equal(t, []byte("left-neighbor"), b.Get([]byte("o/left-neighbor")))
		require.Equal(t, []byte("right-neighbor"), b.Get([]byte("zzz-outside-prefix")))
		return nil
	}))
}

// TestPrefixCursor_ExhaustedIsSticky verifies that once the prefix range is
// exhausted, repeated Next/Prev return nil until an explicit reposition.
func TestPrefixCursor_ExhaustedIsSticky(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")
	const count = 100
	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))

		pc, err := b.PrefixCursor(prefix, bolt.Forward, false)
		require.NoError(t, err)
		var n int
		for k, _, err := pc.First(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			n++
		}
		require.Equal(t, count, n)

		// Sticky: further movement stays at the terminal nil position.
		k, _, err := pc.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		k, _, err = pc.Prev()
		require.NoError(t, err)
		require.Nil(t, k)

		// An explicit Seek re-arms the cursor.
		k, _, err = pc.Seek(prefixedKey(prefix, 5))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, 5), k)
		return nil
	}))
}

// TestPrefixCursor_DeleteLastKey verifies deleting the final matching key and
// then calling Next terminates cleanly without revisiting anything.
func TestPrefixCursor_DeleteLastKey(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	prefix := []byte("p/")
	const count = 100
	setupMultiPageBucket(t, db, []byte("data"), prefix, count)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		pc, err := b.PrefixCursor(prefix, bolt.Forward, true)
		require.NoError(t, err)

		// Position directly on the last key and delete it.
		k, _, err := pc.Seek(prefixedKey(prefix, count-1))
		require.NoError(t, err)
		require.Equal(t, prefixedKey(prefix, count-1), k)
		require.NoError(t, pc.Delete())

		// The successor is outside the prefix, so traversal terminates.
		next, _, err := pc.Next()
		require.NoError(t, err)
		require.Nil(t, next)

		// Sticky termination.
		next, _, err = pc.Next()
		require.NoError(t, err)
		require.Nil(t, next)
		return nil
	}))

	// The remaining 99 keys are intact and still ordered.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		c := b.Cursor()
		survivors := 0
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			require.Equal(t, prefixedKey(prefix, survivors), k)
			survivors++
		}
		require.Equal(t, count-1, survivors)
		return nil
	}))
}

// TestPrefixCursor_NilPrefix verifies a nil prefix iterates every key in the
// bucket, and an all-0xff prefix is handled without an out-of-range successor.
func TestPrefixCursor_NilPrefix(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("a"), []byte("1")))
		require.NoError(t, b.Put([]byte("b"), []byte("2")))
		require.NoError(t, b.Put([]byte("c"), []byte("3")))
		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		pc, err := b.PrefixCursor(nil, bolt.Forward, false)
		require.NoError(t, err)
		var got []string
		for k, _, err := pc.First(); ; k, _, err = pc.Next() {
			require.NoError(t, err)
			if k == nil {
				break
			}
			got = append(got, string(k))
		}
		require.Equal(t, []string{"a", "b", "c"}, got)
		return nil
	}))
}

func expectedPrefixedKeys(prefix []byte, count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = prefixedKey(prefix, i)
	}
	return keys
}

func prefixedKey(prefix []byte, n int) []byte {
	return append(append([]byte{}, prefix...), bigEndianKey(uint64(n))...)
}

func requireAllPrefixedRemoved(t *testing.T, db *btesting.DB, bucket string, prefix []byte, count int) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			t.Fatalf("prefixed key survived cleanup: %x", k)
		}
		// Keys outside the prefix must be untouched.
		require.Equal(t, []byte("right-neighbor"), b.Get([]byte("zzz-outside-prefix")))
		return nil
	}))
}
