package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.etcd.io/bbolt/diagnose"
	"go.etcd.io/bbolt/internal/guts_cli"
)

type doctorOptions struct {
	format string
	json   bool
}

func (o *doctorOptions) normalize() {
	if o.json {
		o.format = "json"
	}
	if o.format == "" {
		o.format = "text"
	}
}

func newDoctorCommand() *cobra.Command {
	var o doctorOptions
	cmd := &cobra.Command{
		Use:          "doctor <bbolt-file>",
		Aliases:      []string{"diagnose", "check-report"},
		Short:        "write a structured, read-only corruption localization report",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			o.normalize()
			return runDoctor(cmd, args[0], o)
		},
	}
	cmd.Flags().StringVar(&o.format, "format", "text", "output format: text|json")
	cmd.Flags().BoolVar(&o.json, "json", false, "shorthand for --format=json")
	return cmd
}

func runDoctor(cmd *cobra.Command, dbPath string, o doctorOptions) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}
	o.normalize()
	if o.format != "text" && o.format != "json" {
		return fmt.Errorf("unsupported report format %q: want text or json", o.format)
	}

	report, err := diagnose.Diagnose(dbPath)
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if o.format == "json" {
		if err := report.JSON(out); err != nil {
			return err
		}
	} else if err := report.Text(out); err != nil {
		return err
	}

	if report.Status == diagnose.StatusCorrupt {
		return guts_cli.ErrCorrupt
	}
	return nil
}
