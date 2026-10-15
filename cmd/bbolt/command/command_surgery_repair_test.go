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
	"go.etcd.io/bbolt/internal/surgeon"
)

func runRepairCmd(t *testing.T, args ...string) {
	t.Helper()
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs(args)
	require.NoError(t, rootCmd.Execute(), args)
}

func TestSurgery_Repair_CrashRecoveryFlow(t *testing.T) {
	const pageSize = 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()
	require.NoError(t,
		db.Fill([]byte("data"), 10, 200,
			func(tx int, k int) []byte { return []byte(fmt.Sprintf("%06d", tx*1000000+k)) },
			func(tx int, k int) []byte { return make([]byte, 24) },
		))
	db.MustClose()
	dstPath := filepath.Join(t.TempDir(), "repaired.db")

	// begin: copy + journal
	runRepairCmd(t, "surgery", "repair", "begin", srcPath, "--output", dstPath)
	assert.FileExists(t, surgeon.JournalPath(dstPath))

	// copy one page (journaled)
	runRepairCmd(t, "surgery", "repair", "copy-page", dstPath, "--from-page", "3", "--to-page", "2")

	// status & verify see one committed page
	runRepairCmd(t, "surgery", "repair", "status", dstPath)
	runRepairCmd(t, "surgery", "repair", "verify", dstPath)

	// crash-style damage to the committed page: recover redoes it
	flipRepairByte(t, dstPath, int64(2*pageSize+200))
	runRepairCmd(t, "surgery", "repair", "recover", dstPath)
	runRepairCmd(t, "surgery", "repair", "verify", dstPath)

	// crash while the commit frame was being persisted: the journal ends
	// with a torn frame, recover rolls page 2 back to its original content
	jpath := surgeon.JournalPath(dstPath)
	raw, err := os.ReadFile(jpath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jpath, raw[:len(raw)-7], 0600))
	runRepairCmd(t, "surgery", "repair", "recover", dstPath)

	srcPage2 := readRepairRaw(t, srcPath, 2, pageSize)
	dstPage2 := readRepairRaw(t, dstPath, 2, pageSize)
	assert.Equal(t, srcPage2, dstPage2)

	// finish: verify + finalize + journal removal; no half-modified page
	runRepairCmd(t, "surgery", "repair", "finish", dstPath)
	assert.NoFileExists(t, jpath)

	// the rolled-back database is byte-identical to the source and healthy
	srcData, err := os.ReadFile(srcPath)
	require.NoError(t, err)
	dstData, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, srcData, dstData)

	reopened, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer reopened.Close()
	require.NoError(t, reopened.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			return err
		}
		return nil
	}))
}

func TestSurgery_Repair_BeginRequiresOutput(t *testing.T) {
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"surgery", "repair", "begin", "db"})
	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "output")
}

func TestSurgery_Repair_ResumeWithoutJournalFails(t *testing.T) {
	db := btesting.MustCreateDB(t)
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"surgery", "repair", "status", db.Path()})
	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no repair journal found")
}

func readRepairRaw(t *testing.T, path string, pgid, pageSize uint64) []byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, pageSize)
	n, err := f.ReadAt(buf, int64(pgid*pageSize))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	return buf
}

func flipRepairByte(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	buf := []byte{0}
	_, err = f.ReadAt(buf, offset)
	require.NoError(t, err)
	buf[0] ^= 0xff
	_, err = f.WriteAt(buf, offset)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}
