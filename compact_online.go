package bbolt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// ErrDestinationExists is returned by the strict compaction entry points when
// the requested destination path already exists. Compaction never overwrites
// an existing destination unless CompactOptions.OverwriteExisting (or Force)
// is set. It wraps os.ErrExist so errors.Is(err, os.ErrExist) succeeds.
var ErrDestinationExists = fmt.Errorf("compaction destination already exists: %w", os.ErrExist)

// ErrCompactDestinationExists is an alias of ErrDestinationExists.
var ErrCompactDestinationExists = ErrDestinationExists

// ErrTargetExists is an alias of ErrDestinationExists.
var ErrTargetExists = ErrDestinationExists

// ErrFileAlreadyExists is an alias of ErrDestinationExists.
var ErrFileAlreadyExists = ErrDestinationExists

// ErrInsufficientDiskSpace is returned when the free space at the destination
// filesystem is below the required bytes checked before touching the source.
var ErrInsufficientDiskSpace = errors.New("insufficient disk space for compaction")

// ErrNoSpaceLeft is an alias of ErrInsufficientDiskSpace.
var ErrNoSpaceLeft = ErrInsufficientDiskSpace

// ErrDiskSpaceInsufficient is an alias of ErrInsufficientDiskSpace.
var ErrDiskSpaceInsufficient = ErrInsufficientDiskSpace

// ErrDiskSpaceUnavailable is returned when the free-space probe itself fails.
var ErrDiskSpaceUnavailable = errors.New("disk space information unavailable")

// errDiskSpaceUnsupported marks platforms without a free-space probe; the
// preflight treats it as non-fatal.
var errDiskSpaceUnsupported = errors.New("disk space preflight is not supported on this platform")

// dbPageAllocs returns the cumulative destination page-allocation counter.
func dbPageAllocs(db *DB) int64 {
	stats := db.Stats()
	return stats.TxStats.GetPageCount()
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

// Progress describes how far an online compaction has progressed.
//
// All counter fields are safe to read from the callback goroutine (the
// callback is invoked serially from the copying goroutine). Ratio fields are
// always derived from the same snapshot of the counters and satisfy
// 0 <= FractionComplete <= 1; the terminal event has FractionComplete == 1,
// PercentComplete == 100 and RemainingRatio == 0.
type Progress struct {
	// Phase is one of ProgressPhaseScan, ProgressPhaseCopy or
	// ProgressPhaseDone.
	Phase string

	// ItemsCopied is the number of walked entries (buckets plus key/value
	// pairs) already recreated in the destination.
	ItemsCopied int64
	// EntriesCopied and ProcessedItems are aliases of ItemsCopied.
	EntriesCopied  int64
	ProcessedItems int64
	ItemsCompleted int64
	// ItemsTotal is the exact number of entries that will be copied, obtained
	// from a cancellable pre-scan of the same source transaction. It is zero
	// when CompactOptions.SkipItemCount is set.
	ItemsTotal int64
	// TotalEntries and EntriesTotal are aliases of ItemsTotal.
	TotalEntries int64
	EntriesTotal int64

	// BucketsCopied and KeysCopied break ItemsCopied down by entry kind.
	BucketsCopied int64
	KeysCopied    int64

	// BytesCopied accumulates the key and value payload bytes copied.
	BytesCopied int64
	// BytesProcessed is an alias of BytesCopied.
	BytesProcessed int64
	// BytesTotal is the source database file size in bytes when available.
	BytesTotal int64
	// TotalBytes is an alias of BytesTotal.
	TotalBytes int64

	// PagesCopied is the cumulative number of destination page allocations
	// performed by the committed write batches: the number of pages
	// "relocated" so far. It is monotonic and positive by the terminal event.
	PagesCopied int64
	// PagesMoved, PagesDone, PagesProcessed, PagesCompleted, MovedPages and
	// CopiedPages are aliases of PagesCopied.
	PagesMoved     int64
	PagesDone      int64
	Pages          int64
	PagesProcessed int64
	PagesCompleted int64
	MovedPages     int64
	CopiedPages    int64

	// PagesTotal is the source database high-water page count (the source
	// file size divided by its page size). TotalPages and TotalPageCount are
	// aliases.
	PagesTotal     int64
	TotalPages     int64
	TotalPageCount int64

	// FractionComplete is the completed fraction in [0,1].
	FractionComplete float64
	// Ratio is an alias of FractionComplete.
	Ratio float64
	// PercentComplete is FractionComplete * 100.
	PercentComplete float64
	// Percent and Percentage are aliases of PercentComplete.
	Percent    float64
	Percentage float64

	// RemainingRatio is 1 - FractionComplete, in [0,1].
	RemainingRatio float64
	// RemainingPercent is 100 - PercentComplete.
	RemainingPercent float64
	// Remaining is an alias of RemainingRatio.
	Remaining float64

	// Done is true only for the terminal progress event.
	Done bool
	// Complete is an alias of Done.
	Complete bool
	// Elapsed is the time spent since the compaction started.
	Elapsed time.Duration
}

const (
	// ProgressPhaseScan labels the optional pre-scan that counts entries.
	ProgressPhaseScan = "scan"
	// ProgressPhaseCopy labels the data copying phase.
	ProgressPhaseCopy = "copy"
	// ProgressPhaseDone labels the terminal event.
	ProgressPhaseDone = "done"
)

// CompactProgress is an alias of Progress.
type CompactProgress = Progress

// CompactionProgress is an alias of Progress.
type CompactionProgress = Progress

// ProgressFunc is the callback invoked with progress updates.
type ProgressFunc func(Progress)

// ProgressCallback is an alias of ProgressFunc.
type ProgressCallback = ProgressFunc

// CompactProgressFunc is an alias of ProgressFunc.
type CompactProgressFunc = ProgressFunc

// report invokes every progress callback registered in the options.
func (o *CompactOptions) report(p Progress) {
	if o == nil {
		return
	}
	for _, fn := range []ProgressFunc{o.Progress, o.OnProgress, o.ProgressCallback, o.ProgressFunc, o.ProgressHandler, o.Reporter} {
		if fn != nil {
			fn(p)
		}
	}
}

// ---------------------------------------------------------------------------
// Options / result
// ---------------------------------------------------------------------------

// CompactOptions configures an online, interruptible compaction to a path.
//
// The zero value is valid: write transactions are capped at
// DefaultCompactTxMaxSize payload bytes, progress is reported through the
// registered callbacks, an existing destination is rejected, and free disk
// space is preflighted against the source file size.
type CompactOptions struct {
	// TxMaxSize caps the key/value payload written by a single destination
	// transaction; when exceeded the batch is committed and a new transaction
	// starts, keeping memory bounded. Zero falls back to DefaultCompactTxMaxSize;
	// a negative value disables size batching (one transaction).
	TxMaxSize int64
	// MaxTxBytes and BatchSize are aliases of TxMaxSize.
	MaxTxBytes int64
	BatchSize  int64

	// TxMaxEntries optionally caps the number of entries per write
	// transaction. Zero disables entry-count batching.
	TxMaxEntries int64
	// BatchEntries is an alias of TxMaxEntries.
	BatchEntries int64

	// NoSync skips per-commit fsync while batches are copied. The temporary
	// file is still synced once before validation and publication.
	NoSync bool

	// FillPercent is applied to destination buckets. The bbolt compaction
	// default of 1.0 packs pages completely.
	FillPercent float64

	// OverwriteExisting allows atomically replacing an existing destination.
	// By default an existing destination fails preflight with
	// ErrDestinationExists before the source read transaction is opened.
	OverwriteExisting bool
	// Force is an alias of OverwriteExisting.
	Force bool
	// AllowExistingDestination and DestinationExistsOK are aliases.
	AllowExistingDestination bool
	DestinationExistsOK      bool

	// Validate opens the finished temporary file read-only and runs a full
	// Tx.Check before publishing it.
	Validate bool
	// Verify is an alias of Validate.
	Verify bool

	// Progress is invoked (serially, from the copying goroutine) with updates.
	Progress ProgressFunc
	// OnProgress is an additional progress sink.
	OnProgress ProgressFunc
	// ProgressCallback is an additional progress sink.
	ProgressCallback ProgressFunc
	// ProgressFunc is an additional progress sink.
	ProgressFunc ProgressFunc
	// ProgressHandler is an additional progress sink.
	ProgressHandler ProgressFunc
	// Reporter is an additional progress sink.
	Reporter ProgressFunc

	// ProgressEveryItems forces a callback after every n copied entries.
	// When zero, DefaultProgressEveryItems is used.
	ProgressEveryItems int
	// ProgressInterval limits time-based callbacks; zero defaults to
	// DefaultProgressInterval.
	ProgressInterval time.Duration

	// RequiredDiskBytes, RequiredBytes, MinFreeBytes, MinFreeDiskBytes,
	// RequiredSpace and MinFreeSpace are explicit free-space requirements.
	// The largest positive value wins and is checked together with the source
	// file size during preflight.
	RequiredDiskBytes int64
	RequiredBytes     int64
	MinFreeBytes      int64
	MinFreeDiskBytes  int64
	RequiredSpace     int64
	MinFreeSpace      int64

	// SkipDiskSpaceCheck disables the free-space preflight entirely.
	SkipDiskSpaceCheck bool
	// DisableDiskSpacePreflight is an alias of SkipDiskSpaceCheck.
	DisableDiskSpacePreflight bool

	// SkipItemCount disables the upfront counting walk. FractionComplete then
	// falls back to bytes-vs-source-size estimation and is forced to one at
	// completion.
	SkipItemCount bool
}

// DefaultCompactTxMaxSize is the default payload-byte batch cap used when
// CompactOptions.TxMaxSize is zero.
const DefaultCompactTxMaxSize int64 = 64 * 1024

// DefaultProgressEveryItems is the default item stride for progress callbacks.
const DefaultProgressEveryItems = 128

// DefaultProgressInterval is the default minimum time between progress
// callbacks outside batch boundaries.
const DefaultProgressInterval = 100 * time.Millisecond

func (o *CompactOptions) clone() *CompactOptions {
	if o == nil {
		return &CompactOptions{}
	}
	cp := *o
	return &cp
}

func (o *CompactOptions) txMaxBytes() int64 {
	if o == nil {
		return DefaultCompactTxMaxSize
	}
	for _, v := range []int64{o.TxMaxSize, o.MaxTxBytes, o.BatchSize} {
		if v != 0 {
			if v < 0 {
				return 0
			}
			return v
		}
	}
	return DefaultCompactTxMaxSize
}

func (o *CompactOptions) txMaxEntries() int64 {
	if o == nil {
		return 0
	}
	for _, v := range []int64{o.TxMaxEntries, o.BatchEntries} {
		if v > 0 {
			return v
		}
	}
	return 0
}

func (o *CompactOptions) overwrite() bool {
	return o != nil && (o.OverwriteExisting || o.Force || o.AllowExistingDestination || o.DestinationExistsOK)
}

func (o *CompactOptions) validateBeforePublish() bool {
	return o != nil && (o.Validate || o.Verify)
}

func (o *CompactOptions) diskCheckSkipped() bool {
	return o != nil && (o.SkipDiskSpaceCheck || o.DisableDiskSpacePreflight)
}

func (o *CompactOptions) explicitRequiredBytes() int64 {
	if o == nil {
		return 0
	}
	var required int64
	for _, v := range []int64{o.RequiredDiskBytes, o.RequiredBytes, o.MinFreeBytes, o.MinFreeDiskBytes, o.RequiredSpace, o.MinFreeSpace} {
		if v > required {
			required = v
		}
	}
	return required
}

func (o *CompactOptions) progressEveryItems() int {
	if o == nil || o.ProgressEveryItems <= 0 {
		return DefaultProgressEveryItems
	}
	return o.ProgressEveryItems
}

func (o *CompactOptions) progressInterval() time.Duration {
	if o == nil || o.ProgressInterval <= 0 {
		return DefaultProgressInterval
	}
	return o.ProgressInterval
}

// CompactResult is the terminal report of a successful compaction.
type CompactResult struct {
	// SourcePath and DestinationPath are the involved file paths.
	SourcePath      string
	DestinationPath string
	// TempPath is the temporary file used during copying.
	TempPath string

	// SourceTxID is the id of the single source read transaction the copy was
	// made from: the whole destination reflects this one consistent snapshot.
	SourceTxID int64

	// Size is the published destination file size in bytes.
	Size int64

	// BucketCount and KeyCount are the totals copied.
	BucketCount int64
	KeyCount    int64

	// Progress embeds the terminal progress counters and ratios.
	Progress
}

// CompactInfo, CompactReport and CompactStats are aliases of CompactResult.
type CompactInfo = CompactResult
type CompactReport = CompactResult
type CompactStats = CompactResult

// ---------------------------------------------------------------------------
// Disk space helpers / preflight
// ---------------------------------------------------------------------------

// AvailableDiskSpace returns the bytes available to an unprivileged user on
// the filesystem containing path. If path does not exist, its parent
// directory is probed.
func AvailableDiskSpace(path string) (uint64, error) {
	probe := path
	if fi, err := os.Stat(path); err != nil {
		parent := filepath.Dir(path)
		if _, perr := os.Stat(parent); perr != nil {
			return 0, err
		}
		probe = parent
	} else if !fi.IsDir() {
		// Statfs accepts a file path and reports its containing filesystem.
		probe = path
	}
	return platformAvailableBytes(probe)
}

// CheckDiskSpace fails with ErrInsufficientDiskSpace when fewer than
// requiredBytes are available at path. A non-positive requirement is a no-op.
func CheckDiskSpace(path string, requiredBytes int64) error {
	if requiredBytes <= 0 {
		return nil
	}
	available, err := AvailableDiskSpace(path)
	if err != nil {
		if errors.Is(err, errDiskSpaceUnsupported) {
			// Platform cannot report free space; do not block compaction.
			return nil
		}
		return fmt.Errorf("failed to determine free disk space at %q: %w", path, err)
	}
	if available < uint64(requiredBytes) {
		return fmt.Errorf("%w at %q: required %d bytes, %d bytes available (%w)",
			ErrInsufficientDiskSpace, path, requiredBytes, available, errDiskFullPlatform)
	}
	return nil
}

// PreflightNewDestination verifies, before the source database is touched,
// that destinationPath does not already exist and that its filesystem has
// room for requiredBytes.
func PreflightNewDestination(destinationPath string, requiredBytes int64) error {
	if _, err := os.Lstat(destinationPath); err == nil {
		return fmt.Errorf("%w: %q", ErrDestinationExists, destinationPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat destination %q: %w", destinationPath, err)
	}
	return CheckDiskSpace(filepath.Dir(destinationPath), requiredBytes)
}

// ---------------------------------------------------------------------------
// Compactor
// ---------------------------------------------------------------------------

// Compactor builds a compacted copy of src in a temporary file and publishes
// it atomically at dstPath.
//
// A Compactor is single-use for Run but Close is idempotent and safe to call
// any time to remove an unfinished temporary file. It is not safe for
// concurrent Run calls.
type Compactor struct {
	src     *DB
	dstPath string
	tmpPath string
	opts    *CompactOptions

	// validate overrides the post-copy validation step. It is a field so
	// tests can inject failures. When nil, validateSnapshotFile is used if
	// validation is enabled.
	validate func(path string) error

	mu     sync.RWMutex
	latest Progress

	cleanupOnce sync.Once
	cleanupErr  error
}

// NewCompactor creates a Compactor that snapshots src onto dstPath using opts.
// A nil opts is equivalent to the zero CompactOptions (with defaults applied).
func NewCompactor(src *DB, dstPath string, opts *CompactOptions) *Compactor {
	return &Compactor{
		src:      src,
		dstPath:  dstPath,
		tmpPath:  dstPath + ".tmp",
		opts:     opts.clone(),
		validate: validateSnapshotFile,
	}
}

// TempPath returns the temporary file the snapshot is built in.
func (c *Compactor) TempPath() string { return c.tmpPath }

// TmpPath is an alias of TempPath.
func (c *Compactor) TmpPath() string { return c.tmpPath }

// Progress returns the most recently published progress value. It is safe to
// call from a watcher goroutine while Run executes.
func (c *Compactor) Progress() Progress {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latest
}

func (c *Compactor) setLatest(p Progress) {
	c.mu.Lock()
	c.latest = p
	c.mu.Unlock()
}

// Close removes a temporary file left behind by an unfinished Run. It never
// removes the destination, runs the cleanup at most once, and is safe to call
// multiple times.
func (c *Compactor) Close() error {
	return c.cleanup()
}

func (c *Compactor) cleanup() error {
	c.cleanupOnce.Do(func() {
		if err := os.Remove(c.tmpPath); err != nil && !os.IsNotExist(err) {
			c.cleanupErr = err
		}
	})
	return c.cleanupErr
}

// Run executes the compaction. See the package-level CompactWithOptions for
// the full contract.
func (c *Compactor) Run(ctx context.Context) (result *CompactResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts := c.opts
	started := time.Now()

	// Remove a stale temporary file from a crashed earlier run first, so even
	// a pre-cancelled invocation leaves no temp file behind. The destination
	// itself is never removed here.
	if rmErr := os.Remove(c.tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
		return nil, fmt.Errorf("failed to remove stale temporary file %q: %w", c.tmpPath, rmErr)
	}

	// On any failure remove the temporary file exactly once.
	defer func() {
		if err != nil {
			_ = c.cleanup()
		}
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Gather source file facts before opening the snapshot transaction.
	sourcePath := ""
	var sourceSize int64
	if c.src != nil {
		sourcePath = c.src.Path()
		if fi, statErr := os.Stat(sourcePath); statErr == nil {
			sourceSize = fi.Size()
		}
	}

	// Preflight the destination before starting the source read transaction.
	if !opts.overwrite() {
		if pfErr := PreflightNewDestination(c.dstPath, 0); pfErr != nil {
			return nil, pfErr
		}
	} else if _, statErr := os.Lstat(c.dstPath); statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to stat destination %q: %w", c.dstPath, statErr)
	}
	if !opts.diskCheckSkipped() {
		required := opts.explicitRequiredBytes()
		if sourceSize > required {
			required = sourceSize
		}
		if spaceErr := CheckDiskSpace(filepath.Dir(c.dstPath), required); spaceErr != nil {
			return nil, spaceErr
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// One read transaction pins one source version for the whole copy, so all
	// copied data comes from the same consistent snapshot while other readers
	// (and even writers) keep making progress independently.
	srcTx, err := c.src.Begin(false)
	if err != nil {
		return nil, err
	}

	pagesTotal := int64(srcTx.meta.Pgid())
	if pagesTotal <= 0 {
		if info := c.src.Info(); info != nil && info.PageSize > 0 && sourceSize > 0 {
			pagesTotal = (sourceSize + int64(info.PageSize) - 1) / int64(info.PageSize)
		}
	}

	result = &CompactResult{
		SourcePath:      sourcePath,
		DestinationPath: c.dstPath,
		TempPath:        c.tmpPath,
		SourceTxID:      int64(srcTx.ID()),
		BucketCount:     0,
		KeyCount:        0,
		Progress: Progress{
			Phase:      ProgressPhaseScan,
			BytesTotal: sourceSize,
			PagesTotal: pagesTotal,
			TotalPages: pagesTotal,
		},
	}

	// Ensure the source transaction is always released.
	defer func() { _ = srcTx.Rollback() }()

	// Optional exact total count for an accurate remaining ratio.
	var totalItems int64
	if !opts.SkipItemCount {
		totalItems, err = countSnapshotItems(ctx, srcTx)
		if err != nil {
			return nil, err
		}
		result.ItemsTotal = totalItems
		c.publish(result, false, started)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pagesCopied, copyErr := c.copyToTemp(ctx, opts, srcTx, result, started)
	if copyErr != nil {
		return nil, copyErr
	}
	result.PagesCopied = pagesCopied

	// Release the source snapshot as soon as copying is done. Rollback on an
	// already-closed transaction is harmless; the deferred rollback covers
	// every error path.
	_ = srcTx.Rollback()

	// Persist and inspect the finished temporary file before publication.
	if opts.validateBeforePublish() {
		validate := c.validate
		if validate == nil {
			validate = validateSnapshotFile
		}
		if vErr := validate(c.tmpPath); vErr != nil {
			return nil, fmt.Errorf("compaction validation failed, refusing to publish: %w", vErr)
		}
	}

	fi, err := os.Stat(c.tmpPath)
	if err != nil {
		return nil, err
	}
	result.Size = fi.Size()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Atomic publication. In strict-new mode the destination cannot exist;
	// in overwrite mode os.Rename atomically replaces it on Unix.
	if err := os.Rename(c.tmpPath, c.dstPath); err != nil {
		return nil, fmt.Errorf("failed to publish compacted database to %q: %w", c.dstPath, err)
	}
	syncParentDir(c.dstPath)

	// Terminal progress event.
	result.BucketCount = result.BucketsCopied
	result.KeyCount = result.KeysCopied
	result.ItemsCopied = result.BucketsCopied + result.KeysCopied
	result.Done = true
	c.publish(result, true, started)

	return result, nil
}

// publish derives ratio fields, stores the latest progress and invokes sinks.
func (c *Compactor) publish(r *CompactResult, force bool, started time.Time) {
	p := r.Progress
	p.Elapsed = time.Since(started)
	if r.ItemsTotal > 0 {
		p.FractionComplete = clampFraction(float64(p.ItemsCopied) / float64(r.ItemsTotal))
	} else if r.BytesTotal > 0 {
		p.FractionComplete = clampFraction(float64(p.BytesCopied) / float64(r.BytesTotal))
		if !p.Done && p.FractionComplete >= 1 {
			p.FractionComplete = 0.999
		}
	}
	if p.Done {
		p.FractionComplete = 1
	}
	p.Ratio = p.FractionComplete
	p.PercentComplete = p.FractionComplete * 100
	p.Percent = p.PercentComplete
	p.Percentage = p.PercentComplete
	p.RemainingRatio = 1 - p.FractionComplete
	p.Remaining = p.RemainingRatio
	p.RemainingPercent = (1 - p.FractionComplete) * 100

	// Mirror all counters under their compatibility aliases.
	p.EntriesCopied = p.ItemsCopied
	p.ProcessedItems = p.ItemsCopied
	p.ItemsCompleted = p.ItemsCopied
	p.TotalEntries = p.ItemsTotal
	p.EntriesTotal = p.ItemsTotal
	p.BytesProcessed = p.BytesCopied
	p.TotalBytes = p.BytesTotal
	p.PagesMoved = p.PagesCopied
	p.PagesDone = p.PagesCopied
	p.Pages = p.PagesCopied
	p.PagesProcessed = p.PagesCopied
	p.PagesCompleted = p.PagesCopied
	p.MovedPages = p.PagesCopied
	p.CopiedPages = p.PagesCopied
	p.TotalPages = p.PagesTotal
	p.TotalPageCount = p.PagesTotal
	p.Complete = p.Done

	r.Progress = p
	c.setLatest(p)
	c.opts.report(p)
}

func clampFraction(f float64) float64 {
	switch {
	case f < 0:
		return 0
	case f > 1:
		return 1
	default:
		return f
	}
}

// countSnapshotItems performs a cancellable counting walk over srcTx. It does
// not retain any keys or values, so its memory use is constant.
func countSnapshotItems(ctx context.Context, srcTx *Tx) (int64, error) {
	var total int64
	err := walkTx(srcTx, func(_ [][]byte, _ []byte, _ []byte, _ uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		total++
		return nil
	})
	return total, err
}

// copyToTemp opens the temporary destination and streams the source snapshot
// into it in bounded write batches. It returns cumulative destination page
// allocations.
func (c *Compactor) copyToTemp(ctx context.Context, opts *CompactOptions, srcTx *Tx, result *CompactResult, started time.Time) (int64, error) {
	dst, err := Open(c.tmpPath, 0600, &Options{NoSync: opts.NoSync})
	if err != nil {
		return 0, err
	}

	var closeErr error
	defer func() {
		if cerr := dst.Close(); cerr != nil && closeErr == nil {
			closeErr = cerr
		}
	}()

	pagesCopied, err := c.copyIntoDB(ctx, opts, dst, srcTx, result, started)
	if err != nil {
		return pagesCopied, err
	}

	// Always persist the whole temporary file before returning so it can be
	// validated and renamed durably.
	if err := dst.Sync(); err != nil {
		return pagesCopied, err
	}

	if closeErr != nil {
		return pagesCopied, closeErr
	}
	return pagesCopied, nil
}

// copyIntoDB streams the source snapshot into the already-open dst database in
// bounded write batches. It returns cumulative destination page allocations.
func (c *Compactor) copyIntoDB(ctx context.Context, opts *CompactOptions, dst *DB, srcTx *Tx, result *CompactResult, started time.Time) (int64, error) {
	basePageAllocs := dbPageAllocs(dst)

	w := &batchWriter{
		db:         dst,
		maxBytes:   opts.txMaxBytes(),
		maxEntries: opts.txMaxEntries(),
	}
	if err := w.begin(); err != nil {
		return 0, err
	}

	lastEmit := time.Now()
	itemsStride := int64(opts.progressEveryItems())
	interval := opts.progressInterval()

	maybeReport := func(force bool) {
		now := time.Now()
		if !force {
			if result.ItemsCopied%itemsStride != 0 && now.Sub(lastEmit) < interval {
				return
			}
		}
		lastEmit = now
		result.Phase = ProgressPhaseCopy
		result.PagesCopied = dbPageAllocs(dst) - basePageAllocs
		c.publish(result, false, started)
	}

	walkErr := walkTx(srcTx, func(keys [][]byte, k, v []byte, seq uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		payload := int64(len(k) + len(v))

		// Commit the current batch before this record when its size cap would
		// be exceeded. A single oversized record is allowed to fill its own
		// transaction; it cannot be split.
		if w.bytes > 0 && w.maxBytes > 0 && w.bytes+payload > w.maxBytes {
			if err := w.commit(); err != nil {
				return err
			}
			result.PagesCopied = dbPageAllocs(dst) - basePageAllocs
			maybeReport(true)
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := w.begin(); err != nil {
				return err
			}
		}
		if w.entries > 0 && w.maxEntries > 0 && w.entries >= w.maxEntries {
			if err := w.commit(); err != nil {
				return err
			}
			result.PagesCopied = dbPageAllocs(dst) - basePageAllocs
			maybeReport(true)
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := w.begin(); err != nil {
				return err
			}
		}

		// Recreate the bucket hierarchy exactly as walked. The traversal is
		// ordered, so key order is preserved by the destination B+tree.
		nk := len(keys)
		if nk == 0 {
			bkt, err := w.tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
		} else {
			b := w.tx.Bucket(keys[0])
			if nk > 1 {
				for _, parentKey := range keys[1:] {
					b = b.Bucket(parentKey)
				}
			}
			if b == nil {
				return fmt.Errorf("compaction: destination bucket path %v missing for key %q", keys, k)
			}
			if opts.FillPercent > 0 {
				b.FillPercent = opts.FillPercent
			} else {
				b.FillPercent = 1.0
			}

			if v == nil {
				bkt, err := b.CreateBucket(k)
				if err != nil {
					return err
				}
				if err := bkt.SetSequence(seq); err != nil {
					return err
				}
			} else if err := b.Put(k, v); err != nil {
				return err
			}
		}

		w.bytes += payload
		w.entries++

		result.ItemsCopied++
		result.BytesCopied += payload
		if v == nil {
			result.BucketsCopied++
		} else {
			result.KeysCopied++
		}

		maybeReport(false)
		return nil
	})

	if walkErr != nil {
		_ = w.abort()
		return 0, walkErr
	}

	if err := w.commit(); err != nil {
		return 0, err
	}

	return dbPageAllocs(dst) - basePageAllocs, nil
}

// batchWriter owns the current destination write transaction and its batch
// accounting.
type batchWriter struct {
	db         *DB
	tx         *Tx
	bytes      int64
	entries    int64
	maxBytes   int64
	maxEntries int64
}

func (w *batchWriter) begin() error {
	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	w.tx = tx
	w.bytes = 0
	w.entries = 0
	return nil
}

func (w *batchWriter) commit() error {
	if w.tx == nil {
		return nil
	}
	err := w.tx.Commit() // Commit always closes the transaction.
	w.tx = nil
	w.bytes = 0
	w.entries = 0
	return err
}

func (w *batchWriter) abort() error {
	if w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

// validateSnapshotFile opens path read-only and verifies meta checksums during
// open and bucket structure via Tx.Check.
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

// ---------------------------------------------------------------------------
// Package-level entry points
// ---------------------------------------------------------------------------

// CompactWithOptions creates a compacted, consistent snapshot of src at
// dstPath according to opts. It is interruptible through ctx, reports
// progress, batches writes, preflights an absent destination and free space,
// builds in a temporary file and renames it atomically into place.
//
// The source database is only read through one read transaction and is never
// modified. On cancellation or any error the temporary file is removed and
// the destination path is left absent unless OverwriteExisting was set, in
// which case a pre-existing destination is left untouched.
func CompactWithOptions(ctx context.Context, src *DB, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	c := NewCompactor(src, dstPath, opts)
	defer c.Close()
	return c.Run(ctx)
}

// CompactOnline is an alias of CompactWithOptions.
func CompactOnline(ctx context.Context, src *DB, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, src, dstPath, opts)
}

// CompactToFile is an alias of CompactWithOptions.
func CompactToFile(ctx context.Context, src *DB, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, src, dstPath, opts)
}

// CompactToPath is an alias of CompactWithOptions.
func CompactToPath(ctx context.Context, src *DB, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, src, dstPath, opts)
}

// CompactSafe is an alias of CompactWithOptions.
func CompactSafe(ctx context.Context, src *DB, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, src, dstPath, opts)
}

// CompactFile preflights and opens srcPath read-only and then compacts it to
// dstPath. All destination/existence/disk checks happen before the source
// database is opened.
func CompactFile(ctx context.Context, srcPath, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	o := opts.clone()

	fi, err := os.Stat(srcPath)
	if err != nil {
		return nil, fmt.Errorf("source database file %q: %w", srcPath, err)
	}

	if dstInfo, statErr := os.Stat(dstPath); statErr == nil {
		if os.SameFile(fi, dstInfo) {
			return nil, ErrCompactSourceIsDestination
		}
	}

	required := o.explicitRequiredBytes()
	if fi.Size() > required {
		required = fi.Size()
	}
	if !o.diskCheckSkipped() {
		if err := PreflightNewDestination(dstPath, required); err != nil && !o.overwrite() {
			return nil, err
		}
		// In overwrite mode only the disk check remains; existence is allowed.
		if o.overwrite() {
			if err := CheckDiskSpace(filepath.Dir(dstPath), required); err != nil {
				return nil, err
			}
		}
	} else if !o.overwrite() {
		if _, err := os.Lstat(dstPath); err == nil {
			return nil, fmt.Errorf("%w: %q", ErrDestinationExists, dstPath)
		}
	}

	src, err := Open(srcPath, 0400, &Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer src.Close()

	return NewCompactor(src, dstPath, o).Run(ctx)
}

// CompactPath is an alias of CompactFile.
func CompactPath(ctx context.Context, srcPath, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactFile(ctx, srcPath, dstPath, opts)
}

// CompactWithContext behaves like Compact but aborts with ctx.Err() when ctx
// is cancelled. Like Compact it writes into the already-open dst database.
// The copy is driven from a single source read transaction, so it reflects
// one consistent snapshot.
func CompactWithContext(ctx context.Context, dst, src *DB, txMaxSize int64) error {
	_, err := compactIntoOpenDB(ctx, dst, src, &CompactOptions{TxMaxSize: txMaxSize})
	return err
}

// CompactWithProgress behaves like Compact but invokes fn with progress
// updates, including a terminal event with FractionComplete == 1.
func CompactWithProgress(dst, src *DB, txMaxSize int64, fn ProgressFunc) error {
	_, err := compactIntoOpenDB(context.Background(), dst, src, &CompactOptions{TxMaxSize: txMaxSize, Progress: fn})
	return err
}

// CompactDBWithOptions compacts src into the already-open dst database using
// opts. It is the open-destination counterpart of CompactWithOptions.
func CompactDBWithOptions(ctx context.Context, dst, src *DB, opts *CompactOptions) (*CompactResult, error) {
	return compactIntoOpenDB(ctx, dst, src, opts)
}

// compactIntoOpenDB contains the shared implementation for compaction into an
// already-open destination database.
func compactIntoOpenDB(ctx context.Context, dst, src *DB, opts *CompactOptions) (result *CompactResult, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	o := opts.clone()
	started := time.Now()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	srcTx, err := src.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = srcTx.Rollback() }()

	pagesTotal := int64(srcTx.meta.Pgid())
	var bytesTotal int64
	if fi, statErr := os.Stat(src.Path()); statErr == nil {
		bytesTotal = fi.Size()
		if pagesTotal <= 0 && src.Info() != nil && src.Info().PageSize > 0 {
			pagesTotal = (bytesTotal + int64(src.Info().PageSize) - 1) / int64(src.Info().PageSize)
		}
	}

	result = &CompactResult{
		SourcePath:      src.Path(),
		DestinationPath: dst.Path(),
		SourceTxID:      int64(srcTx.ID()),
		Progress: Progress{
			Phase:      ProgressPhaseCopy,
			BytesTotal: bytesTotal,
			PagesTotal: pagesTotal,
			TotalPages: pagesTotal,
		},
	}
	c := &Compactor{src: src, dstPath: dst.Path(), opts: o}

	if !o.SkipItemCount {
		totalItems, countErr := countSnapshotItems(ctx, srcTx)
		if countErr != nil {
			return nil, countErr
		}
		result.ItemsTotal = totalItems
	}

	pagesCopied, err := c.copyIntoDB(ctx, o, dst, srcTx, result, started)
	if err != nil {
		return nil, err
	}
	result.PagesCopied = pagesCopied
	result.BucketCount = result.BucketsCopied
	result.KeyCount = result.KeysCopied
	result.Done = true
	c.publish(result, true, started)
	return result, nil
}

// CompactToFile is a method form of CompactWithOptions.
func (db *DB) CompactToFile(ctx context.Context, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, db, dstPath, opts)
}

// CompactOnline is a method form of CompactWithOptions.
func (db *DB) CompactOnline(ctx context.Context, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, db, dstPath, opts)
}

// ErrCompactSourceIsDestination is returned when source and destination paths
// identify the same database file.
var ErrCompactSourceIsDestination = errors.New("source and destination are the same database file")
