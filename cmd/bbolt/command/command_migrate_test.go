package command_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
)

func writeMigrateTable(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

func migrateBucketValue(t *testing.T, path, bucket, key string) string {
	t.Helper()
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var value string
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(bucket))
		if bk == nil {
			return nil
		}
		if v := bk.Get([]byte(key)); v != nil {
			value = string(v)
		}
		return nil
	}))
	return value
}

func TestMigrateCommand_Run(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucket([]byte("objects"))
		if err != nil {
			return err
		}
		if err := bk.Put([]byte("k-a"), []byte("v-a")); err != nil {
			return err
		}
		if err := bk.Put([]byte("k-b"), []byte("v-b")); err != nil {
			return err
		}
		return bk.Put([]byte("k-keep"), []byte("v-keep"))
	}))
	require.NoError(t, db.Close())

	table := writeMigrateTable(t, "table.json", `[
		{"key":"k-a","bucket":"tenant-a"},
		{"key":"6b2d62","bucket":"tenant-b","key_encoding":"hex"}
	]`)

	run := func(args ...string) (string, error) {
		rootCmd := command.NewRootCommand()
		outputBuf := bytes.NewBufferString("")
		rootCmd.SetOut(outputBuf)
		rootCmd.SetArgs(args)
		err := rootCmd.Execute()
		return outputBuf.String(), err
	}

	out, err := run("migrate", db.Path(), "objects", "--table", table, "--batch-id", "1")
	require.NoError(t, err)
	require.Contains(t, out, "moved=2")
	require.Contains(t, out, "batch 1")

	require.Equal(t, "v-a", migrateBucketValue(t, db.Path(), "tenant-a", "k-a"))
	require.Equal(t, "v-b", migrateBucketValue(t, db.Path(), "tenant-b", "k-b"))
	require.Equal(t, "v-keep", migrateBucketValue(t, db.Path(), "objects", "k-keep"))
	require.Empty(t, migrateBucketValue(t, db.Path(), "objects", "k-a"))

	// Re-running the same batch is an idempotent no-op.
	out, err = run("migrate", db.Path(), "objects", "--table", table, "--batch-id", "1")
	require.NoError(t, err)
	require.Contains(t, out, "already applied: no-op")

	// Recovery only.
	out, err = run("migrate", db.Path(), "objects", "--recover")
	require.NoError(t, err)
	require.Contains(t, out, "recovery completed")

	require.Equal(t, "v-a", migrateBucketValue(t, db.Path(), "tenant-a", "k-a"))
}

func TestMigrateCommand_MapTable(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucket([]byte("objects"))
		if err != nil {
			return err
		}
		return bk.Put([]byte("k-a"), []byte("v-a"))
	}))
	require.NoError(t, db.Close())

	table := writeMigrateTable(t, "table.json", `{"k-a":"tenant-a"}`)

	rootCmd := command.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)
	rootCmd.SetArgs([]string{"migrate", db.Path(), "objects", "--table", table})
	require.NoError(t, rootCmd.Execute())
	require.Contains(t, outputBuf.String(), "moved=1")
	require.Equal(t, "v-a", migrateBucketValue(t, db.Path(), "tenant-a", "k-a"))
}

func TestMigrateCommand_MissingKeyFails(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucket([]byte("objects"))
		if err != nil {
			return err
		}
		return bk.Put([]byte("k-a"), []byte("v-a"))
	}))
	require.NoError(t, db.Close())

	table := writeMigrateTable(t, "table.json", `{"ghost":"tenant-a"}`)

	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"migrate", db.Path(), "objects", "--table", table, "--batch-id", "9"})
	err := rootCmd.Execute()
	require.Error(t, err)

	// Nothing moved and the batch was not durably recorded.
	require.Empty(t, migrateBucketValue(t, db.Path(), "tenant-a", "ghost"))
	require.Equal(t, "v-a", migrateBucketValue(t, db.Path(), "objects", "k-a"))
}
