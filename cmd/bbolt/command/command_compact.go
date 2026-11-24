package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type compactOptions struct {
	dstPath   string
	txMaxSize int64
	dstNoSync bool

	// progress controls streaming progress output.
	progress bool
	// overwrite allows replacing an existing destination file.
	overwrite bool
	// verify runs Tx.Check on the temporary file before publishing.
	verify bool
	// requiredBytes optionally raises the free-space preflight requirement.
	requiredBytes int64
	// skipDiskCheck disables the free-space preflight.
	skipDiskCheck bool
}

// Additional command-level sentinels. They alias the bbolt sentinels so tests
// can match either package's error value.
var (
	// ErrCompactDestinationExists is returned when the output path already
	// exists and overwriting was not requested.
	ErrCompactDestinationExists = bolt.ErrDestinationExists
	// ErrCompactInsufficientSpace is returned when the output filesystem does
	// not have enough free space.
	ErrCompactInsufficientSpace = bolt.ErrInsufficientDiskSpace
)

func newCompactCommand() *cobra.Command {
	var o compactOptions
	var compactCmd = &cobra.Command{
		Use:   "compact [options] -o <dst-bbolt-file> <src-bbolt-file>",
		Short: "creates a compacted copy of the database from source path to the destination path, preserving the original.",
		Long: `compact opens a database at source path and walks it recursively, copying keys
as they are found from all buckets, to a newly created database at the destination path.
The original database is left untouched.

By default the destination must not exist yet: the copy is built in a
temporary file next to it, checked and then atomically renamed into place.
If the operation is cancelled or fails, the temporary file is removed and no
half-written destination is left behind. An existing destination can be
replaced with --force/--overwrite. Progress including relocated page count and
remaining percentage is printed unless --progress=false is passed.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(args[0]); err != nil {
				return err
			}
			return o.Run(cmd, args[0])
		},
	}
	o.AddFlags(compactCmd.Flags())

	return compactCmd
}

func (o *compactOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.dstPath, "output", "o", "", "")
	fs.Int64Var(&o.txMaxSize, "tx-max-size", 65536, "")
	fs.BoolVar(&o.dstNoSync, "no-sync", false, "")
	fs.BoolVar(&o.progress, "progress", true, "print progress (relocated pages and remaining percentage)")
	fs.BoolVar(&o.progress, "show-progress", true, "alias of --progress")
	fs.BoolVarP(&o.overwrite, "force", "f", false, "overwrite the output file if it already exists")
	fs.BoolVar(&o.overwrite, "overwrite", false, "alias of --force")
	fs.BoolVar(&o.verify, "verify", false, "verify the compacted file with a full consistency check before publishing")
	fs.Int64Var(&o.requiredBytes, "required-bytes", 0, "additional free-space requirement in bytes for the output filesystem")
	fs.BoolVar(&o.skipDiskCheck, "skip-disk-check", false, "disable the free-space preflight")
	_ = cobra.MarkFlagRequired(fs, "output")
}

func (o *compactOptions) Validate(srcPath string) (err error) {
	if o.dstPath == "" {
		return errors.New("output file required")
	}

	return
}

func (o *compactOptions) Run(cmd *cobra.Command, srcPath string) (err error) {
	// ensure source file exists. Stat-ing the source does not modify it; no
	// database is opened until all destination preflight checks have passed.
	fi, err := checkSourceDBPath(srcPath)
	if err != nil {
		return err
	}

	if dstInfo, statErr := os.Stat(o.dstPath); statErr == nil && os.SameFile(fi, dstInfo) {
		return ErrCompactSourceIsDestination
	}

	// Resolve what to do with a pre-existing destination before opening the
	// source database.
	dstExists := false
	if _, statErr := os.Lstat(o.dstPath); statErr == nil {
		dstExists = true
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	compatEmpty := false
	if dstExists && !o.overwrite {
		// Compatibility: the historical compact command accepted a
		// pre-created but completely empty bbolt file as the output target
		// (some tests and scripts create it first). Such a file contains no
		// data and is safe to replace. Anything else that already exists is a
		// hard preflight failure, before the source is opened.
		if isEmptyBBoltDatabase(o.dstPath) {
			compatEmpty = true
		} else {
			return fmt.Errorf("%w: %q (pass --force to replace it)", ErrCompactDestinationExists, o.dstPath)
		}
	}

	// Free-space preflight. The source size is a conservative lower bound for
	// the output requirement; an explicit --required-bytes can raise it. All
	// checks happen before the source database is opened.
	if !o.skipDiskCheck {
		required := fi.Size()
		if o.requiredBytes > required {
			required = o.requiredBytes
		}
		if spaceErr := bolt.CheckDiskSpace(filepath.Dir(o.dstPath), required); spaceErr != nil {
			return spaceErr
		}
	}

	// Ctrl-C / SIGTERM cancels the compaction context so the temporary file is
	// cleaned up instead of being left half-written.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// open source database read-only: concurrent read transactions are
	// unaffected and the compaction reads one consistent snapshot.
	src, err := bolt.Open(srcPath, 0400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer src.Close()

	compactOpts := &bolt.CompactOptions{
		TxMaxSize:          o.txMaxSize,
		NoSync:             o.dstNoSync,
		OverwriteExisting:  o.overwrite || compatEmpty,
		Validate:           o.verify,
		RequiredDiskBytes:  o.requiredBytes,
		SkipDiskSpaceCheck: o.skipDiskCheck,
		ProgressInterval:   200 * time.Millisecond,
	}
	if o.progress {
		out := cmd.OutOrStdout()
		var lastPrint time.Time
		compactOpts.Progress = func(p bolt.Progress) {
			// The library emits at a fixed item stride as well as on batch
			// boundaries; throttle terminal output to at most ~4 lines/sec,
			// always including the final 100% event.
			now := time.Now()
			if !p.Done && now.Sub(lastPrint) < 250*time.Millisecond {
				return
			}
			lastPrint = now
			fmt.Fprintf(out,
				"compacting: pages=%d/%d entries=%d/%d %.1f%% done, %.1f%% remaining\n",
				p.PagesCopied, p.PagesTotal, p.ItemsCopied, p.ItemsTotal,
				p.FractionComplete*100, p.RemainingRatio*100)
		}
	}

	result, err := bolt.CompactWithOptions(ctx, src, o.dstPath, compactOpts)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(cmd.ErrOrStderr(), "compaction interrupted; temporary output file removed")
		}
		return err
	}

	// report stats on new size.
	newSize := result.Size
	if newSize == 0 {
		// Fallback stat for safety.
		if nfi, statErr := os.Stat(o.dstPath); statErr == nil {
			newSize = nfi.Size()
		}
	}
	if newSize == 0 {
		return fmt.Errorf("zero db size")
	}
	initialSize := fi.Size()
	fmt.Fprintf(cmd.OutOrStdout(), "%d -> %d bytes (gain=%.2fx)\n", initialSize, newSize, float64(initialSize)/float64(newSize))

	return nil
}

// isEmptyBBoltDatabase reports whether path is a valid bbolt database without
// any top-level buckets. It returns false for any unreadable or non-bbolt
// file so callers fail safe.
func isEmptyBBoltDatabase(path string) bool {
	// A short lock timeout keeps this probe from hanging when another handle
	// (typically an in-process test database that has not been closed yet)
	// still holds the file lock: a locked or unreadable target is treated as
	// non-empty and rejected by the caller's preflight.
	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return false
	}
	defer db.Close()

	empty := false
	err = db.View(func(tx *bolt.Tx) error {
		count := 0
		if err := tx.ForEach(func(_ []byte, _ *bolt.Bucket) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		empty = count == 0
		return nil
	})
	return err == nil && empty
}
