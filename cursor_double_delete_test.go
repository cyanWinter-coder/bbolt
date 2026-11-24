package bbolt_test

import (
	"bytes"
	"testing"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestCursor_DoubleDeleteAndMixedMoves(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for _, k := range []string{"a", "b", "c", "d", "e"} {
			if err := b.Put([]byte(k), []byte("v")); err != nil {
				return err
			}
		}

		c := b.Cursor()

		// Seek c, delete it, delete again (slot now holds d), then Next.
		k, _, err := c.Seek([]byte("c"))
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("c")) {
			t.Fatalf("seek c: %s", k)
		}
		if err := c.Delete(); err != nil {
			return err
		}
		// Second delete removes the successor now occupying the slot (d).
		if err := c.Delete(); err != nil {
			return err
		}
		k, _, err = c.Next()
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("e")) {
			t.Fatalf("next after double delete = %s, want e", k)
		}

		// Delete e then Prev must walk over the removed c/d to b.
		if err := c.Delete(); err != nil {
			return err
		}
		k, _, err = c.Prev()
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte("b")) {
			t.Fatalf("prev after delete = %s, want b", k)
		}

		// Remaining: a, b.
		var got []string
		fc := b.Cursor()
		for k, _, err := fc.First(); k != nil; k, _, err = fc.Next() {
			if err != nil {
				return err
			}
			got = append(got, string(k))
		}
		if len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("remaining = %v, want [a b]", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
