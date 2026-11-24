package command_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
)

func uid515Cmd(t *testing.T, args ...string) (*cobra.Command, *bytes.Buffer, error) {
	t.Helper()
	var out bytes.Buffer
	root := command.NewRootCommand()
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return root, &out, err
}

func TestUID515CLI_ExistingGarbageFails(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.db")
	mkUID515DB(t, src)
	dst := filepath.Join(t.TempDir(), "dst.db")
	require.NoError(t, os.WriteFile(dst, []byte("garbage"), 0600))

	_, _, err := uid515Cmd(t, "compact", "-o", dst, src)
	require.Error(t, err)
	require.ErrorIs(t, err, command.ErrCompactDestinationExists)
	require.ErrorIs(t, err, bolt.ErrDestinationExists)
	content, rerr := os.ReadFile(dst)
	require.NoError(t, rerr)
	require.Equal(t, []byte("garbage"), content)
}

func TestUID515CLI_ExistingDataDBFails(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.db")
	mkUID515DB(t, src)
	dst := filepath.Join(t.TempDir(), "dst.db")
	mkUID515DB(t, dst) // non-empty bbolt db

	_, _, err := uid515Cmd(t, "compact", "-o", dst, src)
	require.ErrorIs(t, err, command.ErrCompactDestinationExists)

	// force works
	_, out, err := uid515Cmd(t, "compact", "-o", dst, "--force", src)
	require.NoError(t, err)
	require.Contains(t, out.String(), "gain=")
}

func TestUID515CLI_HappyProgressOutput(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.db")
	mkUID515DB(t, src)
	dst := filepath.Join(t.TempDir(), "nested", "dst.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0755))

	_, out, err := uid515Cmd(t, "compact", "-o", dst, src)
	require.NoError(t, err)
	s := out.String()
	require.Contains(t, s, "remaining")
	require.Contains(t, s, "gain=")
	require.FileExists(t, dst)
	_, statErr := os.Stat(dst + ".tmp")
	require.True(t, os.IsNotExist(statErr))
}

func TestUID515CLI_ExistingOpenDBNoHang(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.db")
	mkUID515DB(t, src)
	dst := filepath.Join(t.TempDir(), "dst.db")
	openDst, err := bolt.Open(dst, 0600, nil)
	require.NoError(t, err)
	defer openDst.Close()

	done := make(chan error, 1)
	go func() {
		_, _, e := uid515Cmd(t, "compact", "-o", dst, src)
		done <- e
	}()
	select {
	case e := <-done:
		require.Error(t, e)
		require.ErrorIs(t, e, command.ErrCompactDestinationExists)
	case <-time.After(10 * time.Second):
		t.Fatal("compact hung probing a locked destination")
	}
}

func mkUID515DB(t *testing.T, path string) {
	t.Helper()
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		for i := 0; i < 200; i++ {
			if err := b.Put([]byte("k"+string(rune('a'+i%26))+string(rune('a'+(i/26)%26))+string(rune('a'+(i/676)%26))), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}
