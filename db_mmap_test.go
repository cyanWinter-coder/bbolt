package bbolt

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// mmapTestFileSize returns the current size of the database file.
func mmapTestFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

func openMmapTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func fillMmapTestDB(t *testing.T, db *DB, keyN, valueSize int) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < keyN; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%06d", i)), make([]byte, valueSize)); err != nil {
				return err
			}
		}
		return nil
	}))
}

// TestDB_MmapRetrySameSizeIsIdempotent ensures that repeating a remap with a
// target size already covered by the current generation is a no-op: the
// mapping, the file size and the read capability all stay untouched.
func TestDB_MmapRetrySameSizeIsIdempotent(t *testing.T) {
	db := openMmapTestDB(t)
	fillMmapTestDB(t, db, 100, 100)

	oldData := db.data
	oldDatasz := db.datasz
	oldFileSize := mmapTestFileSize(t, db.Path())

	// Retry with exactly the current mapping size.
	require.NoError(t, db.mmap(db.datasz))
	assert.True(t, db.data == oldData, "mapping should not be replaced on an idempotent retry")
	assert.Equal(t, oldDatasz, db.datasz)
	assert.Equal(t, oldFileSize, mmapTestFileSize(t, db.Path()), "file must not be extended on an idempotent retry")

	// Retry with a smaller target size.
	require.NoError(t, db.mmap(db.datasz/2))
	assert.True(t, db.data == oldData, "mapping should not be replaced when the target is already covered")
	assert.Equal(t, oldDatasz, db.datasz)
	assert.Equal(t, oldFileSize, mmapTestFileSize(t, db.Path()))

	// The database is still fully usable.
	require.NoError(t, db.View(func(tx *Tx) error {
		v := tx.Bucket([]byte("data")).Get([]byte("000042"))
		assert.Len(t, v, 100)
		return nil
	}))
}

// TestDB_MmapPreCheckFailureKeepsOldGeneration ensures that when the
// pre-flight checks reject a remap (here: the requested size exceeds the
// platform limit), the old mapping and the write capability stay untouched.
func TestDB_MmapPreCheckFailureKeepsOldGeneration(t *testing.T) {
	db := openMmapTestDB(t)
	fillMmapTestDB(t, db, 100, 100)

	oldData := db.data
	oldDatasz := db.datasz
	oldFileSize := mmapTestFileSize(t, db.Path())

	// Request a mapping larger than the platform supports.
	err := db.mmap(common.MaxMapSize + 1)
	require.Error(t, err)
	require.ErrorContains(t, err, "mmap too large")

	assert.True(t, db.data == oldData, "old mapping must stay installed after a rejected remap")
	assert.Equal(t, oldDatasz, db.datasz)
	assert.Equal(t, oldFileSize, mmapTestFileSize(t, db.Path()))

	// Both read and write paths keep working on the old generation.
	require.NoError(t, db.View(func(tx *Tx) error {
		assert.Len(t, tx.Bucket([]byte("data")).Get([]byte("000007")), 100)
		return nil
	}))
	fillMmapTestDB(t, db, 100, 100)
}

// TestDB_MmapSwitchInstallsCompleteNewGeneration ensures a real remap
// atomically installs a complete new generation: bigger mapping, valid meta
// pages, and uninterrupted read/write capability.
func TestDB_MmapSwitchInstallsCompleteNewGeneration(t *testing.T) {
	db := openMmapTestDB(t)
	fillMmapTestDB(t, db, 100, 100)

	oldDatasz := db.datasz

	// Force a remap to a bigger size.
	require.NoError(t, db.mmap(db.datasz+1))
	newDatasz := db.datasz
	require.Greater(t, newDatasz, oldDatasz)

	// Meta pages of the new generation are valid.
	require.NoError(t, db.meta0.Validate())
	require.NoError(t, db.meta1.Validate())

	// Retrying the very same target is a no-op now.
	require.NoError(t, db.mmap(oldDatasz+1))
	assert.Equal(t, newDatasz, db.datasz, "retry with the same target size must not remap again")

	// Data written before and after the switch is readable.
	fillMmapTestDB(t, db, 200, 100)
	require.NoError(t, db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("data"))
		assert.Len(t, b.Get([]byte("000007")), 100)
		assert.Len(t, b.Get([]byte("000199")), 100)
		return nil
	}))
}

// TestDB_GrowIdempotent ensures repeated grows with the same (or a smaller)
// target size never extend the file twice.
func TestDB_GrowIdempotent(t *testing.T) {
	// Use a small AllocSize so that grow actually extends the file beyond
	// the current mapping size.
	db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
	require.NoError(t, err)
	db.AllocSize = 8192
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	fillMmapTestDB(t, db, 100, 100)

	fileSize := mmapTestFileSize(t, db.Path())

	// Growing to the current (or a smaller) size is a no-op.
	require.NoError(t, db.grow(int(fileSize)))
	require.NoError(t, db.grow(int(fileSize)-db.pageSize))
	assert.Equal(t, fileSize, mmapTestFileSize(t, db.Path()))

	// Grow to a bigger size, then repeat the same request.
	target := int(fileSize) + 1<<20
	require.NoError(t, db.grow(target))
	grownSize := mmapTestFileSize(t, db.Path())
	require.Greater(t, grownSize, fileSize)

	require.NoError(t, db.grow(target))
	assert.Equal(t, grownSize, mmapTestFileSize(t, db.Path()), "repeated grow with the same target must not extend the file again")
}

// TestDB_RemapWhileReadTxOpen ensures that an open read transaction keeps
// reading the old, complete mapping generation while a remap is waiting,
// and that the blocked writer finishes deterministically once the reader
// is gone.
func TestDB_RemapWhileReadTxOpen(t *testing.T) {
	db := openMmapTestDB(t)
	fillMmapTestDB(t, db, 100, 100)

	// Pin the current generation with a read transaction.
	rtx, err := db.Begin(false)
	require.NoError(t, err)

	// The writer will block on the mmap lock once it needs to remap.
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *Tx) error {
			b := tx.Bucket([]byte("data"))
			for i := 100; i < 20000; i++ {
				if err := b.Put([]byte(fmt.Sprintf("%06d", i)), make([]byte, 512)); err != nil {
					return err
				}
			}
			return nil
		})
	}()

	// The writer must not be able to finish while the read transaction
	// holds the old generation.
	select {
	case err := <-done:
		t.Fatalf("writer finished while a read transaction was still open: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	// The pinned read transaction still sees the complete old view.
	v := rtx.Bucket([]byte("data")).Get([]byte("000042"))
	require.Len(t, v, 100)
	assert.Nil(t, rtx.Bucket([]byte("data")).Get([]byte("019999")))

	require.NoError(t, rtx.Rollback())

	// The waiting writer now completes; no permanent blocking.
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("writer did not finish after the read transaction was rolled back")
	}

	// New transactions see the complete new generation.
	require.NoError(t, db.View(func(tx *Tx) error {
		assert.Len(t, tx.Bucket([]byte("data")).Get([]byte("019999")), 512)
		return nil
	}))
}

// TestDB_ConcurrentRemapSameTarget ensures concurrent remaps with the same
// target size are serialized by the mmap lock and converge to a single
// deterministic generation: exactly one final mapping size, a stable file
// size and no errors.
func TestDB_ConcurrentRemapSameTarget(t *testing.T) {
	db := openMmapTestDB(t)
	fillMmapTestDB(t, db, 100, 100)

	target := db.datasz + 1

	const workers = 8
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- db.mmap(target)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	finalDatasz := db.datasz
	require.Greater(t, finalDatasz, 0)
	finalFileSize := mmapTestFileSize(t, db.Path())

	// Every concurrent remap converged; repeating it is a no-op for all.
	for i := 0; i < workers; i++ {
		require.NoError(t, db.mmap(target))
	}
	assert.Equal(t, finalDatasz, db.datasz)
	assert.Equal(t, finalFileSize, mmapTestFileSize(t, db.Path()))

	// The database is still fully usable.
	fillMmapTestDB(t, db, 100, 100)
	require.NoError(t, db.View(func(tx *Tx) error {
		assert.Len(t, tx.Bucket([]byte("data")).Get([]byte("000042")), 100)
		return nil
	}))
}

// TestDB_CloseAfterRejectedRemap ensures Close produces a deterministic
// terminal state even after a failed remap attempt.
func TestDB_CloseAfterRejectedRemap(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
	require.NoError(t, err)

	fillMmapTestDB(t, db, 100, 100)
	require.Error(t, db.mmap(common.MaxMapSize+1))

	require.NoError(t, db.Close())
	// Closing an already closed database is a no-op.
	require.NoError(t, db.Close())
}
