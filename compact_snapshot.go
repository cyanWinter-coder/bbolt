package bbolt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SnapshotInfo describes a compacted snapshot published by CompactSnapshot.
type SnapshotInfo struct {
	// SourceTxID is the id of the source read transaction the snapshot was
	// built from. All data in the snapshot is consistent as of this version,
	// regardless of writes committed to the source while it was being built.
	SourceTxID int64
	// BucketCount is the number of buckets copied into the snapshot.
	BucketCount int64
	// KeyCount is the number of key/value pairs copied into the snapshot.
	KeyCount int64
	// Size is the size in bytes of the published snapshot file.
	Size int64
}

// SnapshotCompactOptions configures SnapshotCompactor and CompactSnapshot.
type SnapshotCompactOptions struct {
	// TxMaxSize limits the size of a single write transaction while copying;
	// larger copies are committed in batches. A value of zero disables batching.
	TxMaxSize int64
	// NoSync skips fsync while building the temporary file. The file is still
	// synced once before it is validated and published.
	NoSync bool
}

// SnapshotCompactor builds a compacted copy of a source database from a single
// read transaction and publishes it atomically at the destination path.
//
// The snapshot is first written to a temporary file next to the destination.
// The temporary file is never a valid, openable database for readers of the
// destination path: it is only renamed over the destination after it has been
// fully written, synced and validated (meta checksums and bucket structure).
// Readers of the destination therefore see either the previous complete
// snapshot or the new one, never a partially written file.
//
// A compactor is single-use and is not safe for concurrent use.
type SnapshotCompactor struct {
	src     *DB
	dstPath string
	tmpPath string
	opts    SnapshotCompactOptions

	// validate checks the fully written temporary file before it is
	// published. It is a field so that tests can inject failures.
	validate func(path string) error

	cleanupOnce sync.Once
	cleanupErr  error
}

// NewSnapshotCompactor creates a compactor that snapshots src onto dstPath.
// A nil opts is equivalent to the zero SnapshotCompactOptions.
func NewSnapshotCompactor(src *DB, dstPath string, opts *SnapshotCompactOptions) *SnapshotCompactor {
	c := &SnapshotCompactor{
		src:      src,
		dstPath:  dstPath,
		tmpPath:  dstPath + ".tmp",
		validate: validateSnapshotFile,
	}
	if opts != nil {
		c.opts = *opts
	}
	return c
}

// TmpPath returns the temporary file the snapshot is built in before it is
// published at the destination path.
func (c *SnapshotCompactor) TmpPath() string {
	return c.tmpPath
}

// Run builds the snapshot and atomically publishes it at the destination
// path. The source database may keep accepting writes while Run executes;
// they do not affect this snapshot, and a subsequent compaction simply starts
// from a newer source version.
//
// If the context is cancelled or any step fails, the temporary file is
// removed and a pre-existing destination file is left untouched.
func (c *SnapshotCompactor) Run(ctx context.Context) (info *SnapshotInfo, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Remove a stale temporary file left behind by an interrupted previous
	// run. The destination file itself is never removed here.
	if err := os.Remove(c.tmpPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove stale temp file %q: %w", c.tmpPath, err)
	}

	// On any failure, clean up the temporary file exactly once.
	defer func() {
		if err != nil {
			_ = c.cleanup()
		}
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Copy from a single read transaction so the whole snapshot reflects one
	// source version, no matter how long the copy takes.
	srcTx, err := c.src.Begin(false)
	if err != nil {
		return nil, err
	}
	info = &SnapshotInfo{SourceTxID: int64(srcTx.ID())}
	copyErr := c.copyToTemp(ctx, srcTx, info)
	_ = srcTx.Rollback()
	if copyErr != nil {
		return nil, copyErr
	}

	// The temporary file must prove itself a complete, consistent database
	// before it may be published.
	if err := c.validate(c.tmpPath); err != nil {
		return nil, fmt.Errorf("snapshot validation failed, refusing to publish: %w", err)
	}

	fi, err := os.Stat(c.tmpPath)
	if err != nil {
		return nil, err
	}
	info.Size = fi.Size()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Publish: atomically replace the destination. Concurrent readers see
	// either the old complete file or the new complete file.
	if err := os.Rename(c.tmpPath, c.dstPath); err != nil {
		return nil, fmt.Errorf("failed to publish snapshot: %w", err)
	}
	syncParentDir(c.dstPath)

	return info, nil
}

// Close removes a temporary file left behind by an unfinished Run. It never
// removes the destination file, it runs the cleanup at most once, and it is
// safe to call multiple times.
func (c *SnapshotCompactor) Close() error {
	return c.cleanup()
}

func (c *SnapshotCompactor) cleanup() error {
	c.cleanupOnce.Do(func() {
		if err := os.Remove(c.tmpPath); err != nil && !os.IsNotExist(err) {
			c.cleanupErr = err
		}
	})
	return c.cleanupErr
}

// copyToTemp writes the contents of srcTx into a fresh database at the
// temporary path and syncs it to disk.
func (c *SnapshotCompactor) copyToTemp(ctx context.Context, srcTx *Tx, info *SnapshotInfo) (err error) {
	dst, err := Open(c.tmpPath, 0600, &Options{NoSync: c.opts.NoSync})
	if err != nil {
		return err
	}
	defer func() {
		if tempErr := dst.Close(); tempErr != nil && err == nil {
			err = tempErr
		}
	}()

	if err := compact(dst, srcTx, c.opts.TxMaxSize, ctx, info); err != nil {
		return err
	}

	// The file must be fully persisted before it can be validated and
	// published, even when NoSync was requested for the bulk of the copy.
	return dst.Sync()
}

// validateSnapshotFile opens the database at path and verifies its meta
// checksums (during open) and its bucket structure (via Tx.Check).
func validateSnapshotFile(path string) error {
	db, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.View(func(tx *Tx) error {
		for err := range tx.Check() {
			return err
		}
		return nil
	})
}

// syncParentDir best-effort fsyncs the directory containing path so that a
// rename into it is durable.
func syncParentDir(path string) {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	defer dir.Close()
	_ = dir.Sync()
}

// CompactSnapshot creates a compacted snapshot of src from a single read
// transaction and atomically publishes it at dstPath, replacing any previous
// file there. It returns information about the published snapshot, including
// the source transaction id it was built from.
//
// Unlike Compact, the destination path never contains a partially written
// database: the snapshot is built in a temporary file, validated, and then
// atomically renamed over the destination. If the context is cancelled or an
// error occurs, the temporary file is removed and any pre-existing
// destination file is left untouched.
func CompactSnapshot(ctx context.Context, src *DB, dstPath string, opts *SnapshotCompactOptions) (*SnapshotInfo, error) {
	c := NewSnapshotCompactor(src, dstPath, opts)
	defer c.Close()
	return c.Run(ctx)
}
