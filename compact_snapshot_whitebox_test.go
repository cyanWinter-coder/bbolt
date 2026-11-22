package bbolt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure that a snapshot whose validation fails is never published: the
// temporary file is removed and a pre-existing destination file is untouched.
func TestSnapshotCompactor_ValidationFailure(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "src.db")
	src, err := Open(srcPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, src.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}))
	defer src.Close()

	// Pre-existing destination that must survive the failed run.
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	require.NoError(t, os.WriteFile(dstPath, []byte("previous complete target"), 0600))

	c := NewSnapshotCompactor(src, dstPath, nil)
	injected := errors.New("checksum mismatch")
	c.validate = func(path string) error { return injected }

	_, err = c.Run(context.Background())
	require.ErrorIs(t, err, injected)

	// The invalid temp file must be cleaned up, not published.
	_, serr := os.Stat(c.TmpPath())
	require.True(t, os.IsNotExist(serr), "invalid temp file must not survive")

	content, rerr := os.ReadFile(dstPath)
	require.NoError(t, rerr)
	require.Equal(t, []byte("previous complete target"), content,
		"failed validation must not touch the existing destination")

	require.NoError(t, c.Close())
}

// Ensure the real validator rejects a file that is not a complete database.
func TestValidateSnapshotFile_RejectsGarbage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "garbage.db")
	require.NoError(t, os.WriteFile(path, []byte("not a bbolt database at all"), 0600))
	require.Error(t, validateSnapshotFile(path))
}
