package bbolt_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// fillSnapshotSource writes n transactions into the "data" bucket of db. Each
// transaction i (1-based) stores key-%06d -> i and updates "counter" -> i, so
// any consistent snapshot must contain exactly the keys 1..counter.
func fillSnapshotSource(t *testing.T, db *bolt.DB, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		i := i
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("data"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("%d", i))); err != nil {
				return err
			}
			return b.Put([]byte("counter"), []byte(fmt.Sprintf("%d", i)))
		})
		require.NoError(t, err)
	}
}

// checkSnapshotConsistency verifies that the snapshot at path contains exactly
// the keys 1..counter and passes a full consistency check.
func checkSnapshotConsistency(t *testing.T, path string) int {
	t.Helper()

	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()

	var counter int
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			return err
		}
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		v := b.Get([]byte("counter"))
		require.NotNil(t, v)
		_, err := fmt.Sscanf(string(v), "%d", &counter)
		require.NoError(t, err)
		require.Greater(t, counter, 0)
		for i := 1; i <= counter; i++ {
			require.Equal(t, fmt.Sprintf("%d", i), string(b.Get([]byte(fmt.Sprintf("key-%06d", i)))),
				"snapshot is missing key-%06d although counter is %d", i, counter)
		}
		require.Nil(t, b.Get([]byte(fmt.Sprintf("key-%06d", counter+1))),
			"snapshot contains a key newer than its counter")
		return nil
	}))
	return counter
}

func TestCompactSnapshot_Basic(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	fillSnapshotSource(t, src.DB, 1, 100)

	var srcTxID int
	require.NoError(t, src.View(func(tx *bolt.Tx) error {
		srcTxID = tx.ID()
		return nil
	}))

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	info, err := bolt.CompactSnapshot(context.Background(), src.DB, dstPath, &bolt.SnapshotCompactOptions{TxMaxSize: 1024})
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, int64(srcTxID), info.SourceTxID, "snapshot must record the source transaction id")
	require.Equal(t, int64(101), info.KeyCount, "100 keys + counter")
	require.Equal(t, int64(1), info.BucketCount)
	require.Greater(t, info.Size, int64(0))

	require.Equal(t, 100, checkSnapshotConsistency(t, dstPath))

	// The temporary file must not survive a successful run.
	_, err = os.Stat(dstPath + ".tmp")
	require.True(t, os.IsNotExist(err), "temp file must be gone after publish")
}

func TestCompactSnapshot_ConsistentUnderConcurrentWrites(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	fillSnapshotSource(t, src.DB, 1, 50)

	// Keep writing to the source while the snapshot is being built.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 51; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			i := i
			if err := src.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("data"))
				if err := b.Put([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("%d", i))); err != nil {
					return err
				}
				return b.Put([]byte("counter"), []byte(fmt.Sprintf("%d", i)))
			}); err != nil {
				return
			}
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	info, err := bolt.CompactSnapshot(context.Background(), src.DB, dstPath, nil)
	require.NoError(t, err)

	// The snapshot must be a consistent view of a single source version: all
	// keys up to the recorded counter, and nothing beyond it.
	checkSnapshotConsistency(t, dstPath)
	require.GreaterOrEqual(t, info.SourceTxID, int64(1))
}

func TestCompactSnapshot_ReRunPicksUpNewerVersion(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	fillSnapshotSource(t, src.DB, 1, 20)

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	info1, err := bolt.CompactSnapshot(context.Background(), src.DB, dstPath, nil)
	require.NoError(t, err)
	require.Equal(t, 20, checkSnapshotConsistency(t, dstPath))

	// More writes to the source do not change the published snapshot...
	fillSnapshotSource(t, src.DB, 21, 60)
	require.Equal(t, 20, checkSnapshotConsistency(t, dstPath))

	// ...but a subsequent compaction starts from the newer version.
	info2, err := bolt.CompactSnapshot(context.Background(), src.DB, dstPath, nil)
	require.NoError(t, err)
	require.Greater(t, info2.SourceTxID, info1.SourceTxID)
	require.Equal(t, 60, checkSnapshotConsistency(t, dstPath))
}

func TestCompactSnapshot_Cancel(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	fillSnapshotSource(t, src.DB, 1, 100)

	// Pre-existing destination file that must survive a cancelled run.
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	old := btesting.MustOpenDBWithOption(t, dstPath, nil)
	require.NoError(t, old.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("old"))
		if err != nil {
			return err
		}
		return b.Put([]byte("marker"), []byte("previous"))
	}))
	old.MustClose()
	before, err := os.ReadFile(dstPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = bolt.CompactSnapshot(ctx, src.DB, dstPath, nil)
	require.ErrorIs(t, err, context.Canceled)

	// The old destination must be byte-for-byte intact and no temp file left.
	after, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, before, after, "cancelled snapshot must not touch the existing destination")
	_, err = os.Stat(dstPath + ".tmp")
	require.True(t, os.IsNotExist(err), "temp file must be cleaned up after cancellation")
}

func TestCompactSnapshot_CancelMidCopy(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	// Enough data that the copy takes several batches.
	require.NoError(t, src.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("big"))
		if err != nil {
			return err
		}
		v := make([]byte, 512)
		for i := 0; i < 20000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("key-%06d", i)), v); err != nil {
				return err
			}
		}
		return nil
	}))

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := bolt.CompactSnapshot(ctx, src.DB, dstPath, &bolt.SnapshotCompactOptions{TxMaxSize: 65536})
		errCh <- err
	}()

	// Cancel while the copy is (almost certainly) still running; accept the
	// race where the copy finished first.
	time.Sleep(5 * time.Millisecond)
	cancel()
	err := <-errCh
	if err == nil {
		// Copy won the race: the published snapshot must be complete.
		db, oerr := bolt.Open(dstPath, 0600, &bolt.Options{ReadOnly: true})
		require.NoError(t, oerr)
		require.NoError(t, db.View(func(tx *bolt.Tx) error {
			for err := range tx.Check() {
				return err
			}
			return nil
		}))
		require.NoError(t, db.Close())
		return
	}
	require.ErrorIs(t, err, context.Canceled)
	_, serr := os.Stat(dstPath)
	require.True(t, os.IsNotExist(serr), "no destination may be published on cancellation")
	_, serr = os.Stat(dstPath + ".tmp")
	require.True(t, os.IsNotExist(serr), "temp file must be cleaned up after cancellation")
}

func TestCompactSnapshot_RemovesStaleTempFile(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	fillSnapshotSource(t, src.DB, 1, 10)

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	// Simulate a crashed previous run that left a partial temp file behind.
	require.NoError(t, os.WriteFile(dstPath+".tmp", []byte("partial garbage"), 0600))

	_, err := bolt.CompactSnapshot(context.Background(), src.DB, dstPath, nil)
	require.NoError(t, err)
	require.Equal(t, 10, checkSnapshotConsistency(t, dstPath))
	_, err = os.Stat(dstPath + ".tmp")
	require.True(t, os.IsNotExist(err))
}

func TestSnapshotCompactor_CloseIsIdempotentAndSafe(t *testing.T) {
	src := btesting.MustCreateDB(t)
	defer src.MustClose()
	fillSnapshotSource(t, src.DB, 1, 10)

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	c := bolt.NewSnapshotCompactor(src.DB, dstPath, nil)

	// Close before/without Run: nothing to clean, no error.
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())

	// Failed run (cancelled), then Close: cleanup happens exactly once and
	// never touches the destination.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	_, err = os.Stat(dstPath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(c.TmpPath())
	require.True(t, os.IsNotExist(err))
}
