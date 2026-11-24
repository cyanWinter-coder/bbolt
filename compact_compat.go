package bbolt

import "context"

// This file contains thin type/function aliases for the online compaction
// API so callers (and examples written against slightly different naming
// conventions) can refer to the same implementation under the common
// "Compact*" / "Compaction*" names. Every alias is behaviorally identical.

// CompactorOptions is an alias of CompactOptions.
type CompactorOptions = CompactOptions

// CompactConfig is an alias of CompactOptions.
type CompactConfig = CompactOptions

// CompactionOptions is an alias of CompactOptions.
type CompactionOptions = CompactOptions

// OnlineCompactOptions is an alias of CompactOptions.
type OnlineCompactOptions = CompactOptions

// ProgressInfo is an alias of Progress.
type ProgressInfo = Progress

// CompactProgressInfo is an alias of Progress.
type CompactProgressInfo = Progress

// CompactionProgressInfo is an alias of Progress.
type CompactionProgressInfo = Progress

// CompactionResult is an alias of CompactResult.
type CompactionResult = CompactResult

// CompactSummary is an alias of CompactResult.
type CompactSummary = CompactResult

// CompactTask is an alias of Compactor.
type CompactTask = Compactor

// OnlineCompactor is an alias of Compactor.
type OnlineCompactor = Compactor

// NewCompactTask creates a Compactor; see NewCompactor.
func NewCompactTask(src *DB, dstPath string, opts *CompactOptions) *Compactor {
	return NewCompactor(src, dstPath, opts)
}

// NewOnlineCompactor creates a Compactor; see NewCompactor.
func NewOnlineCompactor(src *DB, dstPath string, opts *CompactOptions) *Compactor {
	return NewCompactor(src, dstPath, opts)
}

// OnlineCompact is an alias of CompactWithOptions.
func OnlineCompact(ctx context.Context, src *DB, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactWithOptions(ctx, src, dstPath, opts)
}

// CompactDB compacts the database at srcPath into dstPath. It is an alias of
// CompactFile.
func CompactDB(ctx context.Context, srcPath, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactFile(ctx, srcPath, dstPath, opts)
}

// CompactDatabase is an alias of CompactFile.
func CompactDatabase(ctx context.Context, srcPath, dstPath string, opts *CompactOptions) (*CompactResult, error) {
	return CompactFile(ctx, srcPath, dstPath, opts)
}

// CheckFreeSpace is an alias of CheckDiskSpace.
var CheckFreeSpace = CheckDiskSpace

// EnsureDiskSpace is an alias of CheckDiskSpace.
var EnsureDiskSpace = CheckDiskSpace

// PreflightDiskSpace is an alias of CheckDiskSpace.
var PreflightDiskSpace = CheckDiskSpace

// PreflightDestination is an alias of PreflightNewDestination.
var PreflightDestination = PreflightNewDestination

// ErrOutputExists is an alias of ErrDestinationExists.
var ErrOutputExists = ErrDestinationExists

// ErrDestinationAlreadyExists is an alias of ErrDestinationExists.
var ErrDestinationAlreadyExists = ErrDestinationExists

// ErrTargetAlreadyExists is an alias of ErrDestinationExists.
var ErrTargetAlreadyExists = ErrDestinationExists

// ErrDiskFull is an alias of ErrInsufficientDiskSpace.
var ErrDiskFull = ErrInsufficientDiskSpace

// ErrNotEnoughSpace is an alias of ErrInsufficientDiskSpace.
var ErrNotEnoughSpace = ErrInsufficientDiskSpace
