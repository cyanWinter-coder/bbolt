package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// End-to-end: stateless reverse pagination with a boundary cursor, while a
// writer deletes keys between page requests. No duplicates, no skipped keys.
func TestCursor_ReversePaginationWithConcurrentDeletes(t *testing.T) {
	db := btesting.MustCreateDB(t)
	const count = 500

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("events"))
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			key := append([]byte("evt/"), cursorTestU64(uint64(i))...)
			if err := b.Put(key, []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const pageSize = 13
	seen := map[string]bool{}
	var token []byte // smallest key returned on the previous page
	first := true

	for page := 0; page < 100; page++ {
		var pageKeys [][]byte
		err := db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket([]byte("events")).Cursor()
			var k []byte
			var err error
			if first {
				k, _, err = c.Last()
			} else {
				// Re-seek at the previous boundary and move off it.
				k, _, err = c.Seek(token)
				if err != nil {
					return err
				}
				k, _, err = c.Prev()
			}
			if err != nil {
				return err
			}
			n := 0
			for n < pageSize && k != nil && bytes.HasPrefix(k, []byte("evt/")) {
				pageKeys = append(pageKeys, bytes.Clone(k))
				token = bytes.Clone(k)
				k, _, err = c.Prev()
				if err != nil {
					return err
				}
				n++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(pageKeys) == 0 {
			break
		}
		for _, k := range pageKeys {
			if seen[string(k)] {
				t.Fatalf("key %q returned on two pages", k)
			}
			seen[string(k)] = true
		}
		first = false

		// Writer deletes a few keys near (and including) the boundary.
		if err := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("events"))
			cur := binary.BigEndian.Uint64(token[len("evt/"):])
			for d := 0; d < 7; d++ {
				if cur < uint64(d) {
					break
				}
				key := append([]byte("evt/"), cursorTestU64(cur-uint64(d))...)
				if err := b.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Every key that still exists must have been observed exactly once, and
	// deleted keys may be absent but must not be double-counted.
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("events"))
		missing := 0
		c := b.Cursor()
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if !seen[string(k)] {
				missing++
			}
		}
		if missing != 0 {
			t.Fatalf("%d surviving keys were never returned", missing)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
