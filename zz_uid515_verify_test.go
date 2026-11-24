package bbolt_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

func uid515FillDeep(t *testing.T, db *bolt.DB) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket([]byte("root"))
		if err != nil {
			return err
		}
		if err := root.SetSequence(42); err != nil {
			return err
		}
		for i := 0; i < 20; i++ {
			if err := root.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i))); err != nil {
				return err
			}
		}
		nested, err := root.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := nested.SetSequence(7); err != nil {
			return err
		}
		for i := 0; i < 30; i++ {
			if err := nested.Put([]byte(fmt.Sprintf("n-%04d", i)), bytes.Repeat([]byte("x"), 128)); err != nil {
				return err
			}
		}
		deep, err := nested.CreateBucket([]byte("deep"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if err := deep.Put([]byte(fmt.Sprintf("d-%02d", i)), []byte{byte(i)}); err != nil {
				return err
			}
		}
		if _, err := tx.CreateBucket([]byte("empty")); err != nil {
			return err
		}
		b2, err := tx.CreateBucket([]byte("orders"))
		if err != nil {
			return err
		}
		for i := 0; i < 500; i++ {
			if err := b2.Put([]byte(fmt.Sprintf("o-%06d", i)), []byte(fmt.Sprintf("payload-%d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
}

type uid515Node struct {
	seq     uint64
	keys    []string
	vals    map[string][]byte
	buckets map[string]*uid515Node
}

func uid515DumpPath(t *testing.T, path string) *uid515Node {
	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var root *uid515Node
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		root = &uid515Node{vals: map[string][]byte{}, buckets: map[string]*uid515Node{}}
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			root.buckets[string(name)] = uid515DumpBucket(b)
			return nil
		})
	}))
	return root
}

func uid515DumpDB(t *testing.T, db *bolt.DB) *uid515Node {
	var root *uid515Node
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		root = &uid515Node{vals: map[string][]byte{}, buckets: map[string]*uid515Node{}}
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			root.buckets[string(name)] = uid515DumpBucket(b)
			return nil
		})
	}))
	return root
}

func uid515DumpBucket(b *bolt.Bucket) *uid515Node {
	n := &uid515Node{seq: b.Sequence(), vals: map[string][]byte{}, buckets: map[string]*uid515Node{}}
	_ = b.ForEach(func(k, v []byte) error {
		ks := string(k)
		n.keys = append(n.keys, ks)
		if v == nil {
			n.buckets[ks] = uid515DumpBucket(b.Bucket(k))
		} else {
			n.vals[ks] = append([]byte(nil), v...)
		}
		return nil
	})
	return n
}

func uid515CompareNode(t *testing.T, path []string, a, b *uid515Node) {
	t.Helper()
	require.Equal(t, a.seq, b.seq, "sequence mismatch at %v", path)
	require.Equal(t, a.keys, b.keys, "key order/content mismatch at %v", path)
	require.Equal(t, len(a.buckets), len(b.buckets), "bucket count mismatch at %v", path)
	for name, an := range a.buckets {
		bn, ok := b.buckets[name]
		require.True(t, ok, "missing bucket %s at %v", name, path)
		uid515CompareNode(t, append(path, name), an, bn)
	}
	for k, v := range a.vals {
		require.Equal(t, v, b.vals[k], "value mismatch key %s at %v", k, path)
	}
	require.True(t, sort.StringsAreSorted(a.keys))
}

// Requirement 2: all buckets, nested buckets, sequences and key order survive.
func TestUID515_FidelityAndOrder(t *testing.T) {
	src := btesting.MustCreateDB(t)
	uid515FillDeep(t, src.DB)

	dst := filepath.Join(t.TempDir(), "compacted.db")
	events := []bolt.Progress{}
	res, err := bolt.CompactWithOptions(context.Background(), src.DB, dst, &bolt.CompactOptions{
		TxMaxSize: 4096,
		Progress:  func(p bolt.Progress) { events = append(events, p) },
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	uid515CompareNode(t, nil, uid515DumpDB(t, src.DB), uid515DumpPath(t, dst))
	require.FileExists(t, dst)
	_, err = os.Stat(dst + ".tmp")
	require.True(t, os.IsNotExist(err))

	require.NotEmpty(t, events)
	last := events[len(events)-1]
	require.True(t, last.Done)
	require.InDelta(t, 1.0, last.FractionComplete, 1e-9)
	require.InDelta(t, 0.0, last.RemainingRatio, 1e-9)
	require.Equal(t, int64(560), last.KeysCopied)  // 20+30+10+500
	require.Equal(t, int64(5), last.BucketsCopied) // root,nested,deep,empty,orders
}

// Requirement 5: progress monotonicity and page counters on a larger db.
func TestUID515_ProgressMonotonic(t *testing.T) {
	src := btesting.MustCreateDB(t)
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

	dst := filepath.Join(t.TempDir(), "dst.db")
	var mu sync.Mutex
	events := []bolt.Progress{}
	res, err := bolt.CompactWithOptions(context.Background(), src.DB, dst, &bolt.CompactOptions{
		TxMaxSize:          65536,
		ProgressEveryItems: 500,
		Progress: func(p bolt.Progress) {
			mu.Lock()
			events = append(events, p)
			mu.Unlock()
		},
	})
	require.NoError(t, err)
	require.Greater(t, res.PagesCopied, int64(0))
	require.Greater(t, res.PagesTotal, int64(0))

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(events), 5)
	var lastItems int64
	var lastFrac float64
	for i, p := range events {
		require.GreaterOrEqual(t, p.ItemsCopied, lastItems, "items not monotonic at %d", i)
		require.GreaterOrEqual(t, p.FractionComplete, lastFrac-1e-12, "fraction not monotonic at %d", i)
		require.GreaterOrEqual(t, p.FractionComplete, 0.0)
		require.LessOrEqual(t, p.FractionComplete, 1.0000001)
		lastItems = p.ItemsCopied
		lastFrac = p.FractionComplete
	}
	require.Equal(t, int64(20000), events[len(events)-1].KeysCopied)
}

// Requirement 6: existing destination fails before source is changed.
func TestUID515_ExistingDestination(t *testing.T) {
	src := btesting.MustCreateDB(t)
	require.NoError(t, src.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucket([]byte("b"))
		return b.Put([]byte("k"), []byte("v"))
	}))
	before, err := os.ReadFile(src.Path())
	require.NoError(t, err)

	// plain pre-existing file
	dst := filepath.Join(t.TempDir(), "exists.db")
	require.NoError(t, os.WriteFile(dst, []byte("do not overwrite me"), 0600))
	_, err = bolt.CompactWithOptions(context.Background(), src.DB, dst, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, bolt.ErrDestinationExists)
	require.ErrorIs(t, err, os.ErrExist)
	afterDst, rerr := os.ReadFile(dst)
	require.NoError(t, rerr)
	require.Equal(t, []byte("do not overwrite me"), afterDst)

	afterSrc, err := os.ReadFile(src.Path())
	require.NoError(t, err)
	require.True(t, bytes.Equal(before, afterSrc), "source changed")

	// CompactFile path-level preflight must also refuse.
	_, err = bolt.CompactFile(context.Background(), src.Path(), dst, nil)
	require.ErrorIs(t, err, bolt.ErrDestinationExists)
}

// Requirement 6: insufficient disk space preflight.
func TestUID515_DiskSpacePreflight(t *testing.T) {
	dir := t.TempDir()
	err := bolt.CheckDiskSpace(dir, 1<<62)
	require.Error(t, err)
	require.ErrorIs(t, err, bolt.ErrInsufficientDiskSpace)

	avail, err := bolt.AvailableDiskSpace(dir)
	require.NoError(t, err)
	require.Greater(t, avail, uint64(0))

	src := btesting.MustCreateDB(t)
	dst := filepath.Join(dir, "x.db")
	_, err = bolt.CompactWithOptions(context.Background(), src.DB, dst, &bolt.CompactOptions{
		MinFreeBytes: 1 << 62,
	})
	require.ErrorIs(t, err, bolt.ErrInsufficientDiskSpace)
	_, statErr := os.Stat(dst)
	require.True(t, os.IsNotExist(statErr))
}

// Requirement 3 + interruptibility: pre-cancelled context leaves nothing.
func TestUID515_PreCanceled(t *testing.T) {
	src := btesting.MustCreateDB(t)
	uid515FillDeep(t, src.DB)
	dst := filepath.Join(t.TempDir(), "dst.db")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := bolt.CompactWithOptions(ctx, src.DB, dst, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(dst)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(dst + ".tmp")
	require.True(t, os.IsNotExist(err))
}

// Requirement 1: snapshot consistency while writes continue.
func TestUID515_ConsistentSnapshotUnderWrites(t *testing.T) {
	src := btesting.MustCreateDB(t)
	require.NoError(t, fillCounter(src.DB, 1, 50))

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

	dst := filepath.Join(t.TempDir(), "dst.db")
	_, err := bolt.CompactWithOptions(context.Background(), src.DB, dst, &bolt.CompactOptions{TxMaxSize: 1024})
	close(stop)
	wg.Wait()
	require.NoError(t, err)
	checkCounterSnapshot(t, dst)
}

func fillCounter(db *bolt.DB, from, to int) error {
	for i := from; i <= to; i++ {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("data"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("%d", i))); err != nil {
				return err
			}
			return b.Put([]byte("counter"), []byte(fmt.Sprintf("%d", i)))
		}); err != nil {
			return err
		}
	}
	return nil
}

func checkCounterSnapshot(t *testing.T, path string) {
	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			return e
		}
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		var counter int
		_, err := fmt.Sscanf(string(b.Get([]byte("counter"))), "%d", &counter)
		require.NoError(t, err)
		for i := 1; i <= counter; i++ {
			require.Equal(t, fmt.Sprintf("%d", i), string(b.Get([]byte(fmt.Sprintf("key-%06d", i)))))
		}
		require.Nil(t, b.Get([]byte(fmt.Sprintf("key-%06d", counter+1))))
		return nil
	}))
}

// Edge cases: empty source and an oversized single value.
func TestUID515_EmptyAndOversized(t *testing.T) {
	empty := btesting.MustCreateDB(t)
	emptyDst := filepath.Join(t.TempDir(), "empty.db")
	res, err := bolt.CompactWithOptions(context.Background(), empty.DB, emptyDst, &bolt.CompactOptions{TxMaxSize: 128})
	require.NoError(t, err)
	require.True(t, res.Done)
	uid515CompareNode(t, nil, uid515DumpDB(t, empty.DB), uid515DumpPath(t, emptyDst))

	big := btesting.MustCreateDB(t)
	require.NoError(t, big.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("huge"), make([]byte, 1<<20)))
		for i := 0; i < 50; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v")))
		}
		return nil
	}))
	bigDst := filepath.Join(t.TempDir(), "big.db")
	_, err = bolt.CompactWithOptions(context.Background(), big.DB, bigDst, &bolt.CompactOptions{TxMaxSize: 4096})
	require.NoError(t, err)
	uid515CompareNode(t, nil, uid515DumpDB(t, big.DB), uid515DumpPath(t, bigDst))
	db, err := bolt.Open(bigDst, 0400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			return e
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

// Cancellation mid-copy must either return context.Canceled and clean up, or
// finish with a valid published db.
func TestUID515_CancelMidCopy(t *testing.T) {
	src := btesting.MustCreateDB(t)
	require.NoError(t, src.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("big"))
		require.NoError(t, err)
		v := make([]byte, 512)
		for i := 0; i < 20000; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("k-%06d", i)), v))
		}
		return nil
	}))

	dst := filepath.Join(t.TempDir(), "dst.db")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, e := bolt.CompactWithOptions(ctx, src.DB, dst, &bolt.CompactOptions{TxMaxSize: 65536})
		errCh <- e
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	err := <-errCh
	if err == nil {
		db, oerr := bolt.Open(dst, 0400, &bolt.Options{ReadOnly: true})
		require.NoError(t, oerr)
		require.NoError(t, db.View(func(tx *bolt.Tx) error {
			for e := range tx.Check() {
				return e
			}
			return nil
		}))
		require.NoError(t, db.Close())
		return
	}
	require.ErrorIs(t, err, context.Canceled)
	_, serr := os.Stat(dst)
	require.True(t, os.IsNotExist(serr))
	_, serr = os.Stat(dst + ".tmp")
	require.True(t, os.IsNotExist(serr))
}
