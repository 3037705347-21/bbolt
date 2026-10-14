package command_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func buildScatteredDB(t *testing.T, abandonFreelist bool, numKeys int) string {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{
		PageSize:       pageSize,
		NoFreelistSync: abandonFreelist,
	})
	require.NoError(t, db.Fill([]byte("data"), 1, numKeys,
		func(_ int, k int) []byte { return []byte(fmt.Sprintf("key-%06d", k)) },
		func(_ int, _ int) []byte { return make([]byte, 200) },
	))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < numKeys; i += 5 {
			require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%06d", i))))
		}
		return nil
	}))
	path := db.Path()
	require.NoError(t, db.Close())
	return path
}

// runBatchedCLIUntilDone invokes the CLI in a loop until the output file is
// produced, and returns the number of invocations it took.
func runBatchedCLIUntilDone(t *testing.T, src, output, state string, batchPages int, maxRuns int) int {
	t.Helper()
	for i := 1; i <= maxRuns; i++ {
		// While work remains, the output must not be created yet.
		if _, err := os.Stat(output); err == nil {
			t.Fatalf("output appeared before the rebuild finished (iteration %d)", i)
		}
		rootCmd := command.NewRootCommand()
		args := []string{
			"surgery", "freelist", "rebuild-batched", src,
			"--output", output,
			"--batch-pages", fmt.Sprintf("%d", batchPages),
		}
		if state != "" {
			args = append(args, "--state", state)
		}
		rootCmd.SetArgs(args)
		require.NoError(t, rootCmd.Execute(), "iteration %d failed", i)

		if _, err := os.Stat(output); err == nil {
			return i
		}
	}
	t.Fatalf("rebuild-batched did not finish within %d runs", maxRuns)
	return 0
}

func TestSurgery_Freelist_RebuildBatched(t *testing.T) {
	srcPath := buildScatteredDB(t, true, 1000)
	srcData := dbData(t, srcPath)
	defer requireDBNoChange(t, srcData, srcPath)

	tmp := t.TempDir()
	output := filepath.Join(tmp, "db")
	state := filepath.Join(tmp, "state.json")

	// One page per invocation forces many resumable batches.
	iterations := runBatchedCLIUntilDone(t, srcPath, output, state, 1, 100000)
	assert.Greater(t, iterations, 1, "rebuild with batch-pages=1 must span multiple invocations")

	// The output must carry a valid persisted freelist.
	meta := readMetaPage(t, output)
	require.NotEqual(t, common.PgidNoFreelist, meta.Freelist())
	assert.Greater(t, meta.Freelist(), common.Pgid(1))
	assert.Less(t, meta.Freelist(), meta.Pgid())

	// bbolt must be able to open the result and find it consistent.
	outDB, err := bolt.Open(output, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, outDB.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			assert.NoError(t, e)
		}
		count := 0
		require.NoError(t, tx.Bucket([]byte("data")).ForEach(func(_, _ []byte) error {
			count++
			return nil
		}))
		assert.Equal(t, 1000-1000/5, count)
		return nil
	}))
	require.NoError(t, outDB.Close())

	// Rerunning the finished command is a no-op (idempotent).
	infoBefore, err := os.Stat(output)
	require.NoError(t, err)
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{
		"surgery", "freelist", "rebuild-batched", srcPath,
		"--output", output,
		"--state", state,
	})
	require.NoError(t, rootCmd.Execute())
	infoAfter, err := os.Stat(output)
	require.NoError(t, err)
	assert.Equal(t, infoBefore.Size(), infoAfter.Size())
}

func TestSurgery_Freelist_RebuildBatched_MismatchStops(t *testing.T) {
	srcPath := buildScatteredDB(t, true, 400)
	srcData := dbData(t, srcPath)
	defer requireDBNoChange(t, srcData, srcPath)

	tmp := t.TempDir()
	corrupt := filepath.Join(tmp, "corrupt.db")
	require.NoError(t, common.CopyFile(srcPath, corrupt))

	// Zero the flags of the root page so the traversal finds a bad type.
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(corrupt)
	require.NoError(t, err)
	root, _, err := guts_cli.GetRootPage(corrupt)
	require.NoError(t, err)
	f, err := os.OpenFile(corrupt, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x00, 0x00}, int64(root)*int64(pageSize)+8)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	output := filepath.Join(tmp, "db")
	state := filepath.Join(tmp, "state.json")

	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{
		"surgery", "freelist", "rebuild-batched", corrupt,
		"--output", output,
		"--state", state,
		"--batch-pages", "4",
	})
	err = rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatch")
	assert.Contains(t, err.Error(), fmt.Sprintf("page %d", root))

	// No output file, and a rerun reports the same mismatch.
	_, err = os.Stat(output)
	assert.True(t, os.IsNotExist(err))

	rootCmd = command.NewRootCommand()
	rootCmd.SetArgs([]string{
		"surgery", "freelist", "rebuild-batched", corrupt,
		"--output", output,
		"--state", state,
		"--batch-pages", "4",
	})
	err = rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatch")
	_, err = os.Stat(output)
	assert.True(t, os.IsNotExist(err))
}

func TestSurgery_Freelist_RebuildBatched_OneShotFlag(t *testing.T) {
	srcPath := buildScatteredDB(t, true, 300)
	srcData := dbData(t, srcPath)
	defer requireDBNoChange(t, srcData, srcPath)

	tmp := t.TempDir()
	output := filepath.Join(tmp, "db")

	// --batch-pages <= 0 finishes the rebuild in a single invocation.
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{
		"surgery", "freelist", "rebuild-batched", srcPath,
		"--output", output,
		"--batch-pages", "0",
	})
	require.NoError(t, rootCmd.Execute())

	meta := readMetaPage(t, output)
	assert.NotEqual(t, common.PgidNoFreelist, meta.Freelist())

	outDB, err := bolt.Open(output, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, outDB.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			assert.NoError(t, e)
		}
		return nil
	}))
	require.NoError(t, outDB.Close())
}
