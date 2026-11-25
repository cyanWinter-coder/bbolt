package diagnose_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/diagnose"
)

func newHealthyDB(t *testing.T, fn func(*testing.T, *bolt.DB)) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	if fn != nil {
		fn(t, db)
	}
	require.NoError(t, db.Close())
	return path
}

func TestDiagnose_EmptyHealthyReport(t *testing.T) {
	path := newHealthyDB(t, nil)
	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)

	require.Equal(t, diagnose.StatusHealthy, rep.Status)
	require.Empty(t, rep.Issues)
	require.Empty(t, rep.BranchStops)
	require.Equal(t, 2, rep.Counts.Categories["meta"].Scanned)
	require.Equal(t, 2, rep.Counts.Categories["meta"].Valid)
	require.Equal(t, 1, rep.Counts.Categories["bucket"].Scanned)
	require.Equal(t, 1, rep.Counts.Categories["leaf_page"].Scanned)
	require.Equal(t, 1, rep.Counts.Categories["freelist"].Scanned)
	require.True(t, rep.Counts.Freelist.Persisted)

	var buf bytes.Buffer
	require.NoError(t, rep.JSON(&buf))
	firstJSON := buf.Bytes()
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(firstJSON, &decoded))
	require.Equal(t, "healthy", decoded["status"])

	rep2, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	var buf2 bytes.Buffer
	require.NoError(t, rep2.JSON(&buf2))
	require.Equal(t, firstJSON, buf2.Bytes())
}

func TestDiagnose_DeepPersistentBucketsHealthy(t *testing.T) {
	path := newHealthyDB(t, func(t *testing.T, db *bolt.DB) {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			a, err := tx.CreateBucket([]byte("a"))
			if err != nil {
				return err
			}
			b, err := a.CreateBucket([]byte("b"))
			if err != nil {
				return err
			}
			c, err := b.CreateBucket([]byte("c"))
			if err != nil {
				return err
			}
			for i := 0; i < 120; i++ {
				key := []byte("deep-" + string(rune('a'+i/26)) + string(rune('a'+i%26)))
				if err := c.Put(key, make([]byte, 96)); err != nil {
					return err
				}
			}
			return nil
		}))
	})

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	for _, issue := range rep.Issues {
		t.Logf("unexpected issue: %+v", issue)
	}
	require.Equal(t, diagnose.StatusHealthy, rep.Status)
	require.Empty(t, rep.Issues)
	require.GreaterOrEqual(t, rep.Counts.Buckets.Total, 4)
}

func TestDiagnose_NoPersistedFreelist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nofreelist.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("x"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}))
	require.NoError(t, db.Close())

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	require.False(t, rep.Counts.Freelist.Persisted)
	require.Empty(t, rep.Issues)
	require.Equal(t, diagnose.StatusHealthy, rep.Status)
}

func TestDiagnose_PopulatedAndInlineBuckets(t *testing.T) {
	path := newHealthyDB(t, func(t *testing.T, db *bolt.DB) {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			root, err := tx.CreateBucket([]byte("widgets"))
			if err != nil {
				return err
			}
			for i := 0; i < 20; i++ {
				if err := root.Put([]byte{byte('a' + i)}, bytes.Repeat([]byte("v"), 32)); err != nil {
					return err
				}
			}
			inner, err := root.CreateBucket([]byte("inline"))
			if err != nil {
				return err
			}
			return inner.Put([]byte("hello"), []byte("world"))
		}))
	})

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	for _, issue := range rep.Issues {
		t.Logf("unexpected issue: %+v", issue)
	}
	require.Equal(t, diagnose.StatusHealthy, rep.Status)
	require.Empty(t, rep.Issues)
	require.GreaterOrEqual(t, rep.Counts.Buckets.Total, 3)
	require.GreaterOrEqual(t, rep.Counts.Buckets.Inline, 1)
	require.GreaterOrEqual(t, rep.Counts.InlinePages, 1)
}
