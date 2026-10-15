package command_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
)

const cliMetaChecksumOff = 16 + 56

func cliCopyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	require.NoError(t, err)
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	defer out.Close()
	_, err = io.Copy(out, in)
	require.NoError(t, err)
}

func cliFlipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer f.Close()
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}

func cliRawPageSize(t *testing.T, path string) uint64 {
	t.Helper()
	size, _, err := guts_cli.ReadPageAndHWMSize(path)
	require.NoError(t, err)
	return size
}

func executeRecover(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	rootCmd := command.NewRootCommand()
	var outBuf, errBuf bytes.Buffer
	rootCmd.SetOut(&outBuf)
	rootCmd.SetErr(&errBuf)
	rootCmd.SetArgs(args)
	err = rootCmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

func TestSurgery_Meta_Recover_Healthy(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		return b.Put([]byte("foo"), []byte("bar"))
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("data")).Put([]byte("foo"), []byte("baz"))
	}))
	db.MustClose()

	original := dbData(t, db.Path())
	dir := t.TempDir()
	out := filepath.Join(dir, "recovered.db")

	stdout, _, err := executeRecover(t, "surgery", "meta", "recover", db.Path(), "--output", out)
	require.NoError(t, err)
	assert.Contains(t, stdout, "Selected meta page")
	assert.Contains(t, stdout, "higher-txid")
	assert.Contains(t, stdout, filepath.Base(out)+".meta-recovery.json")
	requireDBNoChange(t, original, db.Path())

	// Default journal exists and is parseable.
	journal := out + ".meta-recovery.json"
	data, err := os.ReadFile(journal)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"completed"`)

	// Output is a healthy read-only database.
	_, err = chkdb(out)
	require.NoError(t, err)

	// Re-running is an idempotent no-op.
	before := dbData(t, out)
	stdout, _, err = executeRecover(t, "surgery", "meta", "recover", db.Path(), "--output", out)
	require.NoError(t, err)
	assert.Contains(t, stdout, "idempotent re-run")
	assert.Equal(t, before, dbData(t, out))
}

func TestSurgery_Meta_Recover_BothInvalid(t *testing.T) {
	db := btesting.MustCreateDB(t)
	db.MustClose()
	original := dbData(t, db.Path())

	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.db")
	cliCopyFile(t, db.Path(), bad)
	pageSize := cliRawPageSize(t, bad)
	cliFlipByte(t, bad, cliMetaChecksumOff)
	cliFlipByte(t, bad, int64(pageSize)+cliMetaChecksumOff)
	badOriginal := dbData(t, bad)

	out := filepath.Join(dir, "out.db")
	_, stderr, err := executeRecover(t, "surgery", "meta", "recover", bad, "--output", out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both meta pages are invalid")
	assert.Contains(t, err.Error(), "page 0")
	assert.Contains(t, err.Error(), "page 1")
	_ = stderr

	requireDBNoChange(t, original, db.Path())
	requireDBNoChange(t, badOriginal, bad)
	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr), "output must not be created")

	// Aborted journal reports the failure.
	data, readErr := os.ReadFile(out + ".meta-recovery.json")
	require.NoError(t, readErr)
	assert.Contains(t, string(data), `"aborted"`)
}

func TestSurgery_Meta_Recover_AmbiguousNeedsExplicitSelection(t *testing.T) {
	db := btesting.MustCreateDB(t)
	db.MustClose()
	dir := t.TempDir()
	src := filepath.Join(dir, "ambiguous.db")
	cliCopyFile(t, db.Path(), src)
	pageSize := uint32(cliRawPageSize(t, src))

	// Place two checksum-valid metas with the same txid but different
	// rootSequence at positions 0 and 1.
	page0 := readPage(t, src, 0, int(pageSize))
	page1 := append([]byte(nil), page0...)
	common.LoadPage(page1).SetId(1)
	m := common.LoadPageMeta(page1)
	m.RootBucket().SetInSequence(m.RootBucket().InSequence() + 1)
	m.SetChecksum(m.Sum64())
	writePageRaw(t, src, 1, pageSize, page1)

	out := filepath.Join(dir, "out.db")

	// Without --select-meta-page: abort.
	_, _, err := executeRecover(t, "surgery", "meta", "recover", src, "--output", out)
	require.Error(t, err)
	assert.True(t, errors.Is(err, surgeon.ErrMetaPagesAmbiguous) ||
		strings.Contains(err.Error(), surgeon.ErrMetaPagesAmbiguous.Error()))
	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr))

	// Invalid flag value is rejected.
	_, _, err = executeRecover(t, "surgery", "meta", "recover", src, "--output", out, "--select-meta-page", "7")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid --select-meta-page")

	// Explicit selection completes the recovery.
	stdout, _, err := executeRecover(t, "surgery", "meta", "recover", src, "--output", out, "--select-meta-page", "0")
	require.NoError(t, err)
	assert.Contains(t, stdout, "operator-forced")
	_, err = chkdb(out)
	require.NoError(t, err)
}

func TestSurgery_Meta_Recover_ForcedFlagRejectedWhenUnambiguous(t *testing.T) {
	db := btesting.MustCreateDB(t)
	db.MustClose()
	dir := t.TempDir()
	out := filepath.Join(dir, "out.db")

	_, _, err := executeRecover(t, "surgery", "meta", "recover", db.Path(), "--output", out, "--select-meta-page", "0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only allowed when both meta pages are valid")
	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr))
}

func writePageRaw(t *testing.T, path string, id uint64, pageSize uint32, buf []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer f.Close()
	n, err := f.WriteAt(buf, int64(id)*int64(pageSize))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
}
