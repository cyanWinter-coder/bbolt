package bbolt_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// freelistOptions returns the options used by tests in this file. The
// TEST_FREELIST_TYPE env var switches between the array and hashmap backends,
// matching the convention used by the rest of the test suite.
func freelistOptions() *bolt.Options {
	opts := &bolt.Options{}
	if os.Getenv("TEST_FREELIST_TYPE") == "hashmap" {
		opts.FreelistType = bolt.FreelistMapType
	}
	return opts
}

// TestFreelistReclaim_RollbackReusesPages is the end-to-end version of the
// rollback leak fix: pages allocated by rolled back write transactions must
// return to the free set and be reused instead of growing the file.
func TestFreelistReclaim_RollbackReusesPages(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, freelistOptions())

	// Seed a bucket with an initial value so there is a stable root structure.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		return b.Put([]byte("seed"), make([]byte, 4096))
	}))

	before := db.Stats()
	sizeBeforeRollback := fileSize(db.Path())

	// Run a number of write transactions which allocate pages and are then
	// rolled back.
	for i := 0; i < 20; i++ {
		tx, err := db.Begin(true)
		require.NoError(t, err)
		b := tx.Bucket([]byte("widgets"))
		require.NoError(t, b.Put([]byte("rolled-back-key"), make([]byte, 4096)))
		require.NoError(t, tx.Rollback())
	}

	// The rolled back pages must be free again rather than permanently lost.
	afterRollback := db.Stats()
	require.GreaterOrEqual(t, afterRollback.FreePageN, before.FreePageN,
		"rollback must not leak free pages")
	require.Equal(t, sizeBeforeRollback, fileSize(db.Path()),
		"file must not grow while transactions are rolled back")

	// A subsequent committed transaction of similar size should reuse the
	// reclaimed pages instead of allocating new ones.
	tx, err := db.Begin(true)
	require.NoError(t, err)
	require.NoError(t, tx.Bucket([]byte("widgets")).Put([]byte("committed-key"), make([]byte, 4096)))
	require.NoError(t, tx.Commit())

	require.Greater(t, db.Stats().ReusePageN, uint64(0),
		"committed transaction should reuse reclaimed pages")
}

// TestFreelistReclaim_RollbackAcrossReopen verifies the free page accounting
// stays consistent after reopening the database in both freelist sync modes.
func TestFreelistReclaim_RollbackAcrossReopen(t *testing.T) {
	for _, noFreelistSync := range []bool{false, true} {
		t.Run("NoFreelistSync", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			opts := freelistOptions()
			opts.NoFreelistSync = noFreelistSync
			db, err := bolt.Open(path, 0600, opts)
			require.NoError(t, err)

			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("widgets"))
				return err
			}))

			// Allocate then roll back repeatedly.
			for i := 0; i < 10; i++ {
				tx, err := db.Begin(true)
				require.NoError(t, err)
				require.NoError(t, tx.Bucket([]byte("widgets")).Put(
					[]byte("k"), make([]byte, 2048)))
				require.NoError(t, tx.Rollback())
			}

			freeAfterRollback := db.Stats().FreePageN
			require.NoError(t, db.Close())

			// Reopen: free page statistics must be reconstructed from disk
			// (or a full scan in no-sync mode) with the same value.
			reopenOpts := freelistOptions()
			reopenOpts.NoFreelistSync = noFreelistSync
			db2, err := bolt.Open(path, 0600, reopenOpts)
			require.NoError(t, err)
			defer func() { require.NoError(t, db2.Close()) }()

			require.Equal(t, freeAfterRollback, db2.Stats().FreePageN,
				"free page count must stay consistent across reopen")
			require.Equal(t, uint64(0), db2.Stats().ReusePageN,
				"reuse counter is process-local and starts at zero after reopen")
		})
	}
}

// TestFreelistReclaim_ReadOnlyTransactionsReleasePages verifies that closing a
// read-only transaction makes pending pages releasable to writers.
func TestFreelistReclaim_ReadOnlyTransactionsReleasePages(t *testing.T) {
	// A large initial mmap avoids the documented remap-vs-reader deadlock:
	// a writer cannot grow the file while a read transaction holds the mmap
	// read lock.
	opts := freelistOptions()
	opts.InitialMmapSize = os.Getpagesize() * 100
	db := btesting.MustCreateDBWithOption(t, opts)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}))

	// Hold a snapshot open while a writer frees pages. The pages must stay
	// pending until the reader closes, then become allocatable again.
	reader, err := db.Begin(false)
	require.NoError(t, err)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("widgets")).Delete([]byte("k"))
	}))
	require.Greater(t, db.Stats().PendingPageN, 0,
		"freed pages stay pending while a reader holds the old snapshot")

	require.NoError(t, reader.Rollback())

	// Starting a write transaction releases pending pages no reader
	// references any longer.
	tx, err := db.Begin(true)
	require.NoError(t, err)
	require.Equal(t, 0, db.Stats().PendingPageN,
		"pending pages must be released once the last reader closes")
	require.NoError(t, tx.Rollback())
}

// TestFreelistReclaim_PendingPagesNotReallocatedWhileReaderOpen verifies
// requirement 3: pages freed by a writer while a reader still references the
// old snapshot must not be handed out to another writer until that reader
// closes.
func TestFreelistReclaim_PendingPagesNotReallocatedWhileReaderOpen(t *testing.T) {
	opts := freelistOptions()
	opts.InitialMmapSize = os.Getpagesize() * 100
	db := btesting.MustCreateDBWithOption(t, opts)

	// Create enough data to occupy several leaf pages.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < 40; i++ {
			key := []byte(string(rune('a'+i%26)) + string(rune('a'+i/26)))
			if err := b.Put(key, make([]byte, 2048)); err != nil {
				return err
			}
		}
		return nil
	}))

	reader, err := db.Begin(false)
	require.NoError(t, err)

	// Free all the data while the reader keeps the old pages reachable.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 40; i++ {
			key := []byte(string(rune('a'+i%26)) + string(rune('a'+i/26)))
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}))

	// The freed pages must be pending, not available for allocation.
	stats := db.Stats()
	require.Greater(t, stats.PendingPageN, 0)
	require.Equal(t, 0, stats.FreePageN,
		"pages referenced by an open reader must not enter the allocatable set")

	// The open reader must still observe the untouched snapshot.
	require.NoError(t, reader.Bucket([]byte("widgets")).ForEach(func(_, _ []byte) error {
		return nil
	}))
	require.NoError(t, reader.Rollback())

	// After the reader closes, a new write transaction promotes the pages to
	// the free set and can actually reuse them.
	reuseBefore := db.Stats().ReusePageN
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("other"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), make([]byte, 2048))
	}))
	require.Greater(t, db.Stats().ReusePageN, reuseBefore,
		"pages become reusable only once the referencing reader is gone")
}

// TestFreelistReclaim_ReadOnlyOpenErrors verifies read-only open failures and
// write attempts return explicit, documented errors.
func TestFreelistReclaim_ReadOnlyOpenErrors(t *testing.T) {
	// Opening a non-existent file read-only must not create it and must
	// surface the underlying "file does not exist" error.
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	db, err := bolt.Open(missing, 0600, &bolt.Options{ReadOnly: true})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, db)

	// A valid read-only handle rejects write transactions explicitly.
	seed := btesting.MustCreateDB(t)
	require.NoError(t, seed.Close())

	ro, err := bolt.Open(seed.Path(), 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, ro.Close()) }()
	require.True(t, ro.IsReadOnly())

	_, err = ro.Begin(true)
	require.ErrorIs(t, err, berrors.ErrDatabaseReadOnly)
}

// TestFreelistReclaim_StatsExposeReuse verifies the reuse statistic counts
// allocations served from the free list and is exposed through DB.Stats.
func TestFreelistReclaim_StatsExposeReuse(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, freelistOptions())

	// Create some free pages by deleting committed data.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < 50; i++ {
			if err := b.Put([]byte(string(rune('a'+i))), make([]byte, 512)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 50; i++ {
			if err := b.Delete([]byte(string(rune('a' + i)))); err != nil {
				return err
			}
		}
		return nil
	}))

	reuseBefore := db.Stats().ReusePageN

	// A new write transaction should allocate at least part of its pages
	// from the free list.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("other"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), make([]byte, 1024))
	}))

	require.Greater(t, db.Stats().ReusePageN, reuseBefore,
		"ReusePageN must increase when pages are served from the free list")
}
