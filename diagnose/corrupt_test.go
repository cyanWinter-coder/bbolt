package diagnose_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/diagnose"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func hashFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func createFilledDB(t *testing.T, bucket string, keys int) (path string, rootPage uint64) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "corrupt.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{PageSize: 4096})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(bucket))
		if err != nil {
			return err
		}
		for i := 0; i < keys; i++ {
			if err := b.Put([]byte("key-"+string(rune('a'+i/26))+string(rune('a'+i%26))), make([]byte, 96)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		rootPage = uint64(tx.Bucket([]byte(bucket)).RootPage())
		require.NotZero(t, rootPage)
		return nil
	}))
	require.NoError(t, db.Close())
	return path, rootPage
}

func TestDiagnose_DuplicateFreelistPage(t *testing.T) {
	path, _ := createFilledDB(t, "data", 100)

	_, activeMetaPage, err := guts_cli.GetActiveMetaPage(path)
	require.NoError(t, err)
	activeMeta, _, err := guts_cli.GetActiveMetaPage(path)
	require.NoError(t, err)
	freelistPage := uint64(activeMeta.Freelist())
	t.Logf("active meta=%d freelist=%d", activeMetaPage, freelistPage)

	// Rewrite the active freelist page as [4, 4].  Page 4 is an initial root
	// leaf and therefore also becomes reachable-and-freed.
	_, buf, err := guts_cli.ReadPage(path, freelistPage)
	require.NoError(t, err)
	binary.NativeEndian.PutUint16(buf[10:12], 2)
	binary.NativeEndian.PutUint64(buf[16:24], 4)
	binary.NativeEndian.PutUint64(buf[24:32], 4)
	require.NoError(t, guts_cli.WritePage(path, buf))

	before := hashFile(t, path)
	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	require.Equal(t, diagnose.StatusCorrupt, rep.Status)

	var dup *diagnose.Issue
	for i := range rep.Issues {
		if rep.Issues[i].Code == "freelist_duplicate_page" {
			dup = &rep.Issues[i]
			break
		}
	}
	require.NotNil(t, dup)
	require.NotNil(t, dup.PageID)
	require.Equal(t, uint64(4), *dup.PageID)
	require.Equal(t, "freelist", dup.Category)
	require.Equal(t, before, hashFile(t, path), "diagnose must not modify the database")
}

func TestDiagnose_DoubleReferenceHasBucketPathAndKey(t *testing.T) {
	path, rootID := createFilledDB(t, "data", 100)

	p, buf, err := guts_cli.ReadPage(path, rootID)
	require.NoError(t, err)
	require.True(t, p.IsBranchPage())
	require.GreaterOrEqual(t, int(p.Count()), 2)
	firstChild := p.BranchPageElement(0).Pgid()
	secondChild := p.BranchPageElement(1).Pgid()
	secondKey := append([]byte(nil), p.BranchPageElement(1).Key()...)
	p.BranchPageElement(1).SetPgid(firstChild)
	require.NoError(t, guts_cli.WritePage(path, buf))

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	require.Equal(t, diagnose.StatusCorrupt, rep.Status)

	var multi *diagnose.Issue
	for i := range rep.Issues {
		if rep.Issues[i].Code == "page_multiple_references" {
			multi = &rep.Issues[i]
			break
		}
	}
	require.NotNil(t, multi, "expected multiple references in %+v", rep.Issues)
	require.NotNil(t, multi.PageID)
	require.Equal(t, uint64(firstChild), *multi.PageID)
	require.Equal(t, "/data", multi.BucketPath)
	require.NotNil(t, multi.Key)
	require.Equal(t, secondKey, mustHexDecode(t, multi.Key.Hex))
	require.NotEmpty(t, multi.OtherBucketPath)

	var leak *diagnose.Issue
	for i := range rep.Issues {
		if rep.Issues[i].Code == "page_leak" && rep.Issues[i].PageID != nil && *rep.Issues[i].PageID == uint64(secondChild) {
			leak = &rep.Issues[i]
			break
		}
	}
	require.NotNil(t, leak, "displaced child page %d should be reported as leaked", secondChild)
	require.Equal(t, "high", leak.Confidence)
}

func TestDiagnose_CorruptRootReported(t *testing.T) {
	path, rootID := createFilledDB(t, "data", 100)
	p, buf, err := guts_cli.ReadPage(path, rootID)
	require.NoError(t, err)
	p.SetFlags(0)
	require.NoError(t, guts_cli.WritePage(path, buf))

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	t.Logf("root=%d active=%v issues=%+v stops=%+v", rootID, rep.ActiveMetaPage, rep.Issues, rep.BranchStops)

	var rootIssue *diagnose.Issue
	for i := range rep.Issues {
		if rep.Issues[i].Code == "bucket_root_corrupt" {
			rootIssue = &rep.Issues[i]
			break
		}
	}
	require.NotNil(t, rootIssue)
	require.Equal(t, "/data", rootIssue.BucketPath)
	require.NotNil(t, rootIssue.PageID)
	require.Equal(t, rootID, *rootIssue.PageID)
	require.NotEmpty(t, rep.BranchStops)
}

func TestDiagnose_OneUnparseableBucketDoesNotStopSibling(t *testing.T) {
	path := filepath.Join(t.TempDir(), "siblings.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{PageSize: 4096})
	require.NoError(t, err)

	var badRoot, goodRoot uint64
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"bad", "good"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 100; i++ {
				if err := b.Put([]byte("key-"+string(rune('a'+i/26))+string(rune('a'+i%26))), make([]byte, 96)); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		badRoot = uint64(tx.Bucket([]byte("bad")).RootPage())
		goodRoot = uint64(tx.Bucket([]byte("good")).RootPage())
		require.NotZero(t, badRoot)
		require.NotZero(t, goodRoot)
		return nil
	}))
	require.NoError(t, db.Close())

	p, buf, err := guts_cli.ReadPage(path, badRoot)
	require.NoError(t, err)
	p.SetCount(0xffff)
	require.NoError(t, guts_cli.WritePage(path, buf))

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	require.Equal(t, diagnose.StatusCorrupt, rep.Status)

	var stoppedBad bool
	for _, stop := range rep.BranchStops {
		if stop.BucketPath == "/bad" {
			stoppedBad = true
			require.NotEmpty(t, stop.Reason)
		}
	}
	require.True(t, stoppedBad, "expected /bad branch stop: %+v", rep.BranchStops)

	var sawGoodBucket bool
	for _, issue := range rep.Issues {
		require.NotEqual(t, "/good", issue.BucketPath, "healthy sibling produced issue")
	}
	require.GreaterOrEqual(t, rep.Counts.Buckets.Total, 3)
	_ = sawGoodBucket
}

func TestDiagnose_DoesNotBlockOpenReadTransaction(t *testing.T) {
	path, _ := createFilledDB(t, "data", 20)
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_ = db.View(func(tx *bolt.Tx) error {
			close(entered)
			<-release
			return nil
		})
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("read transaction did not start")
	}

	reportDone := make(chan struct{})
	go func() {
		_, err := diagnose.Diagnose(path)
		require.NoError(t, err)
		close(reportDone)
	}()
	select {
	case <-reportDone:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("diagnose blocked while another read transaction was active")
	}
	close(release)
	<-done
}

func TestDiagnose_InvalidMetaChecksumStillReports(t *testing.T) {
	path, _ := createFilledDB(t, "data", 20)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// Meta page 0 checksum occupies bytes 72:80.
	data[79] ^= 0xff
	require.NoError(t, os.WriteFile(path, data, 0600))

	rep, err := diagnose.Diagnose(path)
	require.NoError(t, err)
	var metaIssue *diagnose.Issue
	for i := range rep.Issues {
		if rep.Issues[i].Category == "meta" && rep.Issues[i].PageID != nil && *rep.Issues[i].PageID == 0 {
			metaIssue = &rep.Issues[i]
			break
		}
	}
	require.NotNil(t, metaIssue)
	require.Equal(t, diagnose.StatusCorrupt, rep.Status)
	require.NotNil(t, rep.ActiveMetaPage)
	require.Equal(t, uint64(1), *rep.ActiveMetaPage)
}

func TestDiagnose_RandomCorruptionDoesNotPanic(t *testing.T) {
	path, _ := createFilledDB(t, "data", 120)
	original, err := os.ReadFile(path)
	require.NoError(t, err)

	for seed := int64(0); seed < 80; seed++ {
		corrupt := append([]byte(nil), original...)
		rng := rand.New(rand.NewSource(seed))
		for j := 0; j < 1+int(seed%7); j++ {
			offset := rng.Intn(len(corrupt))
			corrupt[offset] ^= byte(1 + rng.Intn(255))
		}
		corruptPath := filepath.Join(t.TempDir(), "random.db")
		require.NoError(t, os.WriteFile(corruptPath, corrupt, 0600))

		var rep *diagnose.Report
		require.NotPanics(t, func() {
			var diagErr error
			rep, diagErr = diagnose.Diagnose(corruptPath)
			require.NoError(t, diagErr)
		}, "seed %d", seed)
		require.NotNil(t, rep)
		_, err := json.Marshal(rep)
		require.NoError(t, err, "seed %d", seed)
	}
}

func mustHexDecode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
