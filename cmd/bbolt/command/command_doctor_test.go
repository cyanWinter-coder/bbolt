package command_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/diagnose"
	"go.etcd.io/bbolt/internal/btesting"
)

func executeCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	rootCmd := command.NewRootCommand()
	output := &bytes.Buffer{}
	rootCmd.SetOut(output)
	rootCmd.SetErr(output)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return output.String(), err
}

func TestDoctorCommand_JSONHealthy(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Close())

	out, err := executeCommand(t, "doctor", db.Path(), "--format", "json")
	require.NoError(t, err)

	var rep diagnose.Report
	require.NoError(t, json.Unmarshal([]byte(out), &rep))
	require.Equal(t, diagnose.StatusHealthy, rep.Status)
	require.Equal(t, 2, rep.Counts.Categories["meta"].Scanned)
}

func TestCheckCommand_JSONReport(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Close())

	out, err := executeCommand(t, "check", db.Path(), "--json")
	require.NoError(t, err)
	require.Contains(t, out, `"schema_version": 1`)
	require.Contains(t, out, `"status": "healthy"`)
}
