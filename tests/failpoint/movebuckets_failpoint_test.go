package failpoint

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	gofail "go.etcd.io/gofail/runtime"
)

// TestFailpoint_MoveBuckets_CommitFailureRestoresOriginalPosition verifies the
// atomicity guarantee of Tx.MoveBuckets when the transaction fails while
// writing the meta page: every source, target and nested bucket must remain in
// its original position because the transaction is rolled back wholesale.
func TestFailpoint_MoveBuckets_CommitFailureRestoresOriginalPosition(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
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
		deep, err := b.CreateBucket([]byte("deep"))
		if err != nil {
			return err
		}
		if err := deep.Put([]byte("dk"), []byte("dv")); err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("x"))
		return err
	}))

	before := dumpBucketTree(t, db)

	t.Log("Enable the beforeWriteMetaError failpoint.")
	require.NoError(t, gofail.Enable("beforeWriteMetaError", `return("writeMeta somehow failed")`))

	t.Log("Batch move must fail when the meta page cannot be written.")
	err := db.Update(func(tx *bolt.Tx) error {
		return tx.MoveBuckets([]bolt.MoveBucketSpec{
			{Key: []byte("b"), Src: tx.Bucket([]byte("a")), Dst: tx.Bucket([]byte("x"))},
		})
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "writeMeta somehow failed")

	t.Log("Disable the failpoint before verifying recovery.")
	require.NoError(t, gofail.Disable("beforeWriteMetaError"))

	t.Log("All buckets must remain at their original positions after rollback.")
	after := dumpBucketTree(t, db)
	require.Equal(t, before, after)

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		// Source path intact, including the nested bucket and sequence.
		b := tx.Bucket([]byte("a")).Bucket([]byte("b"))
		require.NotNil(t, b)
		require.Equal(t, uint64(7), b.Sequence())
		require.Equal(t, []byte("dv"), b.Bucket([]byte("deep")).Get([]byte("dk")))

		// Nothing landed in the destination.
		require.Nil(t, tx.Bucket([]byte("x")).Bucket([]byte("b")))
		return nil
	}))

	t.Log("The database must still accept transactions after the failure.")
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("a")).Bucket([]byte("b")).Put([]byte("recovered"), []byte("ok"))
	}))
}

// dumpBucketTree renders bucket paths with sequence and plain key/value pairs
// so before/after structural comparisons are independent of meta page layout.
func dumpBucketTree(t *testing.T, db *btesting.DB) string {
	t.Helper()
	var out string
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			dumpBucketNode(b, "/"+string(name), &out)
			return nil
		})
	}))
	return out
}

func dumpBucketNode(b *bolt.Bucket, prefix string, out *string) {
	*out += prefix + " seq=" + strconv.FormatUint(b.Sequence(), 10) + "\n"
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil {
			dumpBucketNode(b.Bucket(k), prefix+"/"+string(k), out)
			continue
		}
		*out += prefix + " " + string(k) + "=" + string(v) + "\n"
	}
}
