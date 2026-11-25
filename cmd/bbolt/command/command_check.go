package command

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/guts_cli"
)

type checkOptions struct {
	fromPageID uint64
	report     bool
	format     string
	json       bool
}

func (o *checkOptions) AddFlags(fs *pflag.FlagSet) {
	fs.Uint64VarP(&o.fromPageID, "from-page", "", o.fromPageID, "check db integrity starting from the given page ID")
	fs.BoolVar(&o.report, "report", false, "emit a categorized report with bucket paths and first bad keys")
	fs.StringVar(&o.format, "format", "text", "report output format when --report is set: text|json")
	fs.BoolVar(&o.json, "json", false, "shorthand for --report --format=json")
}

func newCheckCommand() *cobra.Command {
	var o checkOptions
	checkCmd := &cobra.Command{
		Use:          "check <bbolt-file>",
		Short:        "verify integrity of bbolt database data",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return checkFunc(cmd, args[0], o)
		},
	}

	o.AddFlags(checkCmd.Flags())
	return checkCmd
}

func checkFunc(cmd *cobra.Command, dbPath string, cfg checkOptions) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	if cfg.json || cfg.format == "json" {
		cfg.report = true
		cfg.format = "json"
	}
	if cfg.report {
		if cfg.fromPageID != 0 {
			return fmt.Errorf("--report checks all buckets and pages and cannot be combined with --from-page")
		}
		return runDoctor(cmd, dbPath, doctorOptions{format: cfg.format, json: cfg.json})
	}

	// Open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	opts := []bolt.CheckOption{bolt.WithKVStringer(CmdKvStringer())}
	if cfg.fromPageID != 0 {
		opts = append(opts, bolt.WithPageId(cfg.fromPageID))
	}
	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		for err := range tx.Check(opts...) {
			fmt.Fprintln(cmd.OutOrStdout(), err)
			count++
		}

		// Print summary of errors.
		if count > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "%d errors found\n", count)
			return guts_cli.ErrCorrupt
		}

		// Notify user that database is valid.
		fmt.Fprintln(cmd.OutOrStdout(), "OK")
		return nil
	})
}
