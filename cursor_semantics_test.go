package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

func cursorTestU64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Requirement 1: forward and reverse Seek/iteration return consistent
// (empty) results on empty, nested and inline buckets.
func TestCursor_ConsistentAcrossBucketKinds(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// A freshly created bucket stays inline until it grows large enough.
	if err := db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket([]byte("root"))
		if err != nil {
			return err
		}
		// inline + empty
		if _, err := root.CreateBucket([]byte("emptyInline")); err != nil {
			return err
		}
		// inline with data
		in, err := root.CreateBucket([]byte("dataInline"))
		if err != nil {
			return err
		}
		for _, k := range []string{"a", "b", "c"} {
			if err := in.Put([]byte(k), []byte(k)); err != nil {
				return err
			}
		}
		// nested bucket holding a sub-bucket
		nested, err := root.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if _, err := nested.CreateBucket([]byte("child")); err != nil {
			return err
		}
		// a bucket large enough to be backed by its own (non-inline) page
		big, err := root.CreateBucket([]byte("big"))
		if err != nil {
			return err
		}
		for i := 0; i < 1000; i++ {
			if err := big.Put(cursorTestU64(uint64(i)), make([]byte, 100)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte("root"))

		// Empty inline bucket: every entry point must report nil and no error.
		empty := root.Bucket([]byte("emptyInline"))
		if empty == nil {
			t.Fatal("expected empty inline bucket")
		}
		for name, call := range map[string]func() ([]byte, []byte, error){
			"First": empty.Cursor().First,
			"Last":  empty.Cursor().Last,
		} {
			k, v, err := call()
			if err != nil {
				t.Fatalf("%s on empty bucket returned error: %v", name, err)
			}
			if k != nil || v != nil {
				t.Fatalf("%s on empty bucket returned %q/%q, want nil", name, k, v)
			}
		}
		c := empty.Cursor()
		if k, _, err := c.Seek([]byte("anything")); err != nil || k != nil {
			t.Fatalf("Seek on empty bucket: k=%q err=%v, want nil/nil", k, err)
		}
		if k, _, err := c.SeekPrefix([]byte("any"), nil); err != nil || k != nil {
			t.Fatalf("SeekPrefix on empty bucket: k=%q err=%v, want nil/nil", k, err)
		}
		// First-then-Next and Last-then-Prev must both terminate with nil.
		fc := empty.Cursor()
		if k, _, err := fc.First(); err != nil || k != nil {
			t.Fatalf("empty First: %q %v", k, err)
		}
		if k, _, err := fc.Next(); err != nil || k != nil {
			t.Fatalf("empty Next: %q %v", k, err)
		}
		lc := empty.Cursor()
		if k, _, err := lc.Last(); err != nil || k != nil {
			t.Fatalf("empty Last: %q %v", k, err)
		}
		if k, _, err := lc.Prev(); err != nil || k != nil {
			t.Fatalf("empty Prev: %q %v", k, err)
		}

		// Inline bucket with data: forward and reverse walks agree.
		want := []string{"a", "b", "c"}
		in := root.Bucket([]byte("dataInline"))
		var gotFwd, gotRev []string
		ic := in.Cursor()
		for k, _, err := ic.First(); k != nil; k, _, err = ic.Next() {
			if err != nil {
				return err
			}
			gotFwd = append(gotFwd, string(k))
		}
		ic2 := in.Cursor()
		for k, _, err := ic2.Last(); k != nil; k, _, err = ic2.Prev() {
			if err != nil {
				return err
			}
			gotRev = append(gotRev, string(k))
		}
		if fmt.Sprint(gotFwd) != fmt.Sprint(want) {
			t.Fatalf("inline forward = %v, want %v", gotFwd, want)
		}
		if fmt.Sprint(gotRev) != fmt.Sprint([]string{"c", "b", "a"}) {
			t.Fatalf("inline reverse = %v, want [c b a]", gotRev)
		}
		// Seek semantics identical on inline buckets.
		if k, _, err := in.Cursor().Seek([]byte("b")); err != nil || !bytes.Equal(k, []byte("b")) {
			t.Fatalf("inline Seek(b) = %q, %v", k, err)
		}
		if k, _, err := in.Cursor().Seek([]byte("aa")); err != nil || !bytes.Equal(k, []byte("b")) {
			t.Fatalf("inline Seek(aa) = %q, %v (want b)", k, err)
		}

		// Nested bucket exposes its child through the cursor with a nil value.
		nested := root.Bucket([]byte("nested"))
		k, v, err := nested.Cursor().First()
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("child")) || v != nil {
			t.Fatalf("nested cursor = %q/%q, want child/nil", k, v)
		}

		// Large (paged) bucket: reverse walk must not stop at empty pages.
		big := root.Bucket([]byte("big"))
		var n int
		bc := big.Cursor()
		for k, _, err := bc.Last(); k != nil; k, _, err = bc.Prev() {
			if err != nil {
				return err
			}
			n++
		}
		if n != 1000 {
			t.Fatalf("paged reverse count = %d, want 1000", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Requirement 2: after deleting the current key, Next lands on the successor
// and Prev on the predecessor with no duplicate or skipped key.
func TestCursor_DeleteThenNextAndPrev(t *testing.T) {
	db := btesting.MustCreateDB(t)

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for _, k := range []string{"a", "b", "c", "d"} {
			if err := b.Put([]byte(k), []byte("v")); err != nil {
				return err
			}
		}

		// Delete then Next lands on the successor.
		c := b.Cursor()
		if k, _, err := c.First(); err != nil || !bytes.Equal(k, []byte("a")) {
			t.Fatalf("First = %q, %v", k, err)
		}
		if err := c.Delete(); err != nil {
			return err
		}
		if k, _, err := c.Next(); err != nil || !bytes.Equal(k, []byte("b")) {
			t.Fatalf("Next after delete = %q, %v (want b)", k, err)
		}
		if k, _, err := c.Next(); err != nil || !bytes.Equal(k, []byte("c")) {
			t.Fatalf("second Next = %q, %v (want c)", k, err)
		}

		// Delete then Prev lands on the predecessor.
		if err := c.Delete(); err != nil {
			return err
		}
		if k, _, err := c.Prev(); err != nil || !bytes.Equal(k, []byte("b")) {
			t.Fatalf("Prev after delete = %q, %v (want b)", k, err)
		}
		// Iterating forward again must not revisit or skip anything.
		var got []string
		fc := b.Cursor()
		for k, _, err := fc.First(); k != nil; k, _, err = fc.Next() {
			if err != nil {
				return err
			}
			got = append(got, string(k))
		}
		if fmt.Sprint(got) != fmt.Sprint([]string{"b", "d"}) {
			t.Fatalf("remaining keys = %v, want [b d]", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Requirement 2 across leaf-page boundaries: deleting every key while
// iterating in either direction must visit each key exactly once, even when
// deletes empty out whole leaf pages.
func TestCursor_DeleteWhileIterating_Paged(t *testing.T) {
	const count = 1000
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			db := btesting.MustCreateDB(t)
			if err := db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("widgets"))
				if err != nil {
					return err
				}
				for i := 0; i < count; i++ {
					if err := b.Put(cursorTestU64(uint64(i)), make([]byte, 100)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if err := db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("widgets"))
				c := b.Cursor()
				seen := make(map[uint64]bool)

				if reverse {
					for k, _, err := c.Last(); k != nil; k, _, err = c.Prev() {
						if err != nil {
							return err
						}
						seen[binary.BigEndian.Uint64(k)] = true
						if err := c.Delete(); err != nil {
							return err
						}
					}
				} else {
					for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
						if err != nil {
							return err
						}
						seen[binary.BigEndian.Uint64(k)] = true
						if err := c.Delete(); err != nil {
							return err
						}
					}
				}

				if len(seen) != count {
					return fmt.Errorf("visited %d unique keys, want %d", len(seen), count)
				}
				// Every key must be gone.
				for i := 0; i < count; i++ {
					if b.Get(cursorTestU64(uint64(i))) != nil {
						return fmt.Errorf("key %d still present", i)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Reverse iteration over a tree whose deletes left empty leaf pages in the
// middle must not terminate early or revisit keys.
func TestCursor_ReverseIterationWithEmptyPages(t *testing.T) {
	db := btesting.MustCreateDB(t)
	const count = 1000

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if err := b.Put(cursorTestU64(uint64(i)), make([]byte, 100)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		ranges := [][2]int{{200, 300}, {500, 600}, {800, 900}}
		for _, rng := range ranges {
			for i := rng[0]; i < rng[1]; i++ {
				if err := b.Delete(cursorTestU64(uint64(i))); err != nil {
					return err
				}
			}
		}

		deleted := func(i int) bool {
			for _, rng := range ranges {
				if i >= rng[0] && i < rng[1] {
					return true
				}
			}
			return false
		}
		var want []uint64
		for i := count - 1; i >= 0; i-- {
			if !deleted(i) {
				want = append(want, uint64(i))
			}
		}

		c := b.Cursor()
		var got []uint64
		for k, _, err := c.Last(); k != nil; k, _, err = c.Prev() {
			if err != nil {
				return err
			}
			got = append(got, binary.BigEndian.Uint64(k))
		}
		if len(got) != len(want) {
			t.Fatalf("reverse over empty pages: got %d keys, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("reverse mismatch at %d: got %d want %d", i, got[i], want[i])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Reverse iteration over a tree whose deletes leave several consecutive
// empty leaf pages must not terminate early or revisit keys.
func TestCursor_ReverseIterationWithConsecutiveEmptyPages(t *testing.T) {
	db := btesting.MustCreateDB(t)
	const count = 2000

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if err := b.Put(cursorTestU64(uint64(i)), make([]byte, 100)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		// Delete the whole middle so consecutive leaf pages become empty,
		// keeping only survivors at both ends of the key space.
		for i := 300; i < 1700; i++ {
			if err := b.Delete(cursorTestU64(uint64(i))); err != nil {
				return err
			}
		}

		var want []uint64
		for i := count - 1; i >= 0; i-- {
			if i < 300 || i >= 1700 {
				want = append(want, uint64(i))
			}
		}

		c := b.Cursor()
		var got []uint64
		for k, _, err := c.Last(); k != nil; k, _, err = c.Prev() {
			if err != nil {
				return err
			}
			got = append(got, binary.BigEndian.Uint64(k))
		}
		if len(got) != len(want) {
			t.Fatalf("got %d keys, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("mismatch at %d: got %d want %d", i, got[i], want[i])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Requirement 3: every cursor entry point returns ErrTxClosed (not a panic,
// not freed-page data) once the owning transaction has ended.
func TestCursor_AfterTxClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatal(err)
	}

	check := func(t *testing.T, c *bolt.Cursor) {
		t.Helper()
		if _, _, err := c.First(); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("First after close: err = %v, want ErrTxClosed", err)
		}
		if _, _, err := c.Last(); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("Last after close: err = %v, want ErrTxClosed", err)
		}
		if _, _, err := c.Next(); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("Next after close: err = %v, want ErrTxClosed", err)
		}
		if _, _, err := c.Prev(); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("Prev after close: err = %v, want ErrTxClosed", err)
		}
		if _, _, err := c.Seek([]byte("k")); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("Seek after close: err = %v, want ErrTxClosed", err)
		}
		if _, _, err := c.SeekPrefix([]byte("k"), nil); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("SeekPrefix after close: err = %v, want ErrTxClosed", err)
		}
		if err := c.Delete(); !errors.Is(err, berrors.ErrTxClosed) {
			t.Fatalf("Delete after close: err = %v, want ErrTxClosed", err)
		}
	}

	// After rollback of a read-only tx.
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	readCursor := tx.Bucket([]byte("widgets")).Cursor()
	if k, _, err := readCursor.First(); err != nil || !bytes.Equal(k, []byte("k")) {
		t.Fatalf("First before close: %q, %v", k, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	t.Run("rollback", func(t *testing.T) { check(t, readCursor) })

	// After commit of a read-write tx.
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	writeCursor := tx.Bucket([]byte("widgets")).Cursor()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	t.Run("commit", func(t *testing.T) { check(t, writeCursor) })
}

// Requirement 4: SeekPrefix honors an exclusive end boundary and the prefix.
func TestCursor_SeekPrefix(t *testing.T) {
	db := btesting.MustCreateDB(t)
	keys := []string{
		"a/1", "a/2", "a/3",
		"b/1", "b/2",
		"c/1",
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err := b.Put([]byte(k), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))

		// No end bound: the whole prefix range, and keys outside the
		// prefix are never returned by the initial seek.
		c := b.Cursor()
		k, _, err := c.SeekPrefix([]byte("a/"), nil)
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("a/1")) {
			t.Fatalf("SeekPrefix(a/) = %q, want a/1", k)
		}
		var got []string
		for k != nil && bytes.HasPrefix(k, []byte("a/")) {
			got = append(got, string(k))
			k, _, err = c.Next()
			if err != nil {
				return err
			}
		}
		if fmt.Sprint(got) != fmt.Sprint([]string{"a/1", "a/2", "a/3"}) {
			t.Fatalf("prefix walk = %v", got)
		}

		// Exclusive end bound: the boundary key itself is excluded.
		c = b.Cursor()
		k, _, err = c.SeekPrefix([]byte("a/"), []byte("a/3"))
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("a/1")) {
			t.Fatalf("bounded SeekPrefix = %q, want a/1", k)
		}
		got = nil
		for k != nil && bytes.Compare(k, []byte("a/3")) < 0 {
			got = append(got, string(k))
			k, _, err = c.Next()
			if err != nil {
				return err
			}
		}
		if fmt.Sprint(got) != fmt.Sprint([]string{"a/1", "a/2"}) {
			t.Fatalf("bounded walk = %v, want [a/1 a/2]", got)
		}

		// End bound equal to an existing key excludes that key.
		c = b.Cursor()
		k, _, err = c.SeekPrefix([]byte("b/"), []byte("b/2"))
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("b/1")) {
			t.Fatalf("SeekPrefix(b/,b/2) = %q, want b/1", k)
		}

		// End bound smaller than prefix: nothing qualifies.
		if k, _, err := b.Cursor().SeekPrefix([]byte("c/"), []byte("a/9")); err != nil {
			return err
		} else if k != nil {
			t.Fatalf("end before prefix returned %q, want nil", k)
		}

		// Non-matching prefix with no end bound returns nil.
		if k, _, err := b.Cursor().SeekPrefix([]byte("zzz"), nil); err != nil {
			return err
		} else if k != nil {
			t.Fatalf("missing prefix returned %q, want nil", k)
		}

		// End bound beyond the last key walks to the natural end.
		c = b.Cursor()
		var all []string
		for k, _, err := c.SeekPrefix([]byte(""), []byte("zzz")); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			all = append(all, string(k))
		}
		if len(all) != len(keys) {
			t.Fatalf("wide range returned %d keys, want %d", len(all), len(keys))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
