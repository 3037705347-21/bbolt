package surgeon_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	fl2 "go.etcd.io/bbolt/internal/freelist"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
)

// buildFragmentedDB creates a database with several buckets, lots of pages
// and then deletes most of the keys so the free space is highly scattered.
// When abandonFreelist is true the database is written without a persisted
// freelist. The returned DB is already closed; the caller owns the file.
func buildFragmentedDB(t *testing.T, pageSize int, abandonFreelist bool, numKeys int) string {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{
		PageSize:       pageSize,
		NoFreelistSync: abandonFreelist,
	})

	require.NoError(t, db.Fill([]byte("data"), 1, numKeys,
		func(_ int, k int) []byte { return []byte(fmt.Sprintf("key-%06d", k)) },
		func(_ int, _ int) []byte { return make([]byte, 200) },
	))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		parent, err := tx.CreateBucketIfNotExists([]byte("widgets"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucketIfNotExists([]byte("child"))
		if err != nil {
			return err
		}
		for i := 0; i < numKeys/4; i++ {
			if err := child.Put([]byte(fmt.Sprintf("w-%06d", i)), make([]byte, 150)); err != nil {
				return err
			}
		}
		return nil
	}))
	// Delete roughly 80% of the keys, spread all over the key range.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < numKeys; i += 5 {
			if err := b.Delete([]byte(fmt.Sprintf("key-%06d", i))); err != nil {
				return err
			}
		}
		c := tx.Bucket([]byte("widgets")).Bucket([]byte("child"))
		for i := 0; i < numKeys/4; i += 5 {
			if err := c.Delete([]byte(fmt.Sprintf("w-%06d", i))); err != nil {
				return err
			}
		}
		return nil
	}))

	path := db.Path()
	require.NoError(t, db.Close())
	return path
}

func fileHash(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func runRebuildLoop(t *testing.T, opts surgeon.RebuildFreelistOptions, batchPages int, stopAfter int) *surgeon.RebuildProgress {
	t.Helper()
	opts.BatchPages = batchPages
	var prog *surgeon.RebuildProgress
	for i := 0; ; i++ {
		p, err := surgeon.RunRebuildFreelist(opts)
		require.NoError(t, err, "rebuild iteration %d failed", i)
		prog = p
		if p.Done || (stopAfter > 0 && i+1 >= stopAfter) {
			return prog
		}
		// While sweeping, every registered free page id must be unique and
		// sorted (a re-run after a crash must not register a page twice).
		if p.Phase == surgeon.RebuildPhaseSweep {
			st := readRebuildStateForTest(t, opts.StatePath)
			for j := 1; j < len(st.FreePageIDs); j++ {
				require.Less(t, st.FreePageIDs[j-1], st.FreePageIDs[j],
					"duplicated/unsorted free page id in checkpoint")
			}
		}
		if i > 1_000_000 {
			t.Fatalf("rebuild did not converge")
		}
	}
}

// rebuildStateMirror is a test-only mirror of the persisted checkpoint that
// exposes the fields the tests need to inspect.
type rebuildStateMirror struct {
	Phase       string        `json:"phase"`
	FreePageIDs []common.Pgid `json:"free_page_ids"`
	SweptUntil  common.Pgid   `json:"swept_until"`
	HWMPgid     common.Pgid   `json:"hwm_pgid"`
}

func readRebuildStateForTest(t *testing.T, path string) rebuildStateMirror {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var st rebuildStateMirror
	require.NoError(t, json.Unmarshal(data, &st))
	return st
}

// assertOutputHealthy opens the rebuilt database read-only, verifies bbolt
// consistency (which detects reachable-freed pages) and that surviving data
// is still there.
func assertOutputHealthy(t *testing.T, output string, expectedKeys int) {
	t.Helper()
	db, err := bolt.Open(output, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		count := 0
		for err := range tx.Check() {
			assert.NoError(t, err)
		}
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		require.NoError(t, b.ForEach(func(_, _ []byte) error { count++; return nil }))
		assert.Equal(t, expectedKeys, count)
		return nil
	}))
}

// freePageCountReadOnly opens a database read-only, forces the freelist to
// load (reconstructing it by a scan when it was abandoned) and reports the
// number of free pages bbolt itself computes.
func freePageCountReadOnly(t *testing.T, path string) int {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, NoFreelistSync: true})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			assert.NoError(t, err)
		}
		return nil
	}))
	return db.Stats().FreePageN
}

func TestRebuildFreelist_BatchedAndResumable(t *testing.T) {
	const numKeys = 1000
	src := buildFragmentedDB(t, 4096, true, numKeys)
	before := fileHash(t, src)

	tmp := t.TempDir()
	output := filepath.Join(tmp, "rebuilt.db")
	state := filepath.Join(tmp, "state.json")
	opts := surgeon.RebuildFreelistOptions{SourcePath: src, OutputPath: output, StatePath: state}

	// Do a few tiny batches; the output must not exist yet, but progress
	// must be persisted so an interrupted run can resume.
	partial := runRebuildLoop(t, opts, 1, 3)
	assert.False(t, partial.Done)
	_, err := os.Stat(output)
	assert.True(t, os.IsNotExist(err), "output must only be created in the final phase")
	_, err = os.Stat(state)
	assert.NoError(t, err)

	// Finish the rebuild with small batches.
	prog := runRebuildLoop(t, opts, 3, 0)
	assert.True(t, prog.Done)
	assert.Greater(t, prog.FreePages, 0)

	// The source file must be byte-identical: scanning is strictly
	// read-only with respect to the source.
	assert.Equal(t, before, fileHash(t, src))

	// Surviving keys: 1000 - 200 deleted (every 5th).
	assertOutputHealthy(t, output, numKeys-numKeys/5)

	// The rebuilt free set must be exactly what bbolt computes itself when
	// reconstructing the freelist from reachability.
	assert.Equal(t, freePageCountReadOnly(t, src), prog.FreePages)
	assert.Equal(t, freePageCountReadOnly(t, output), prog.FreePages)
}

func TestRebuildFreelist_OneShotEqualsBatched(t *testing.T) {
	src := buildFragmentedDB(t, 4096, true, 800)
	tmp := t.TempDir()

	outOneShot := filepath.Join(tmp, "one.db")
	progOne := runRebuildLoop(t, surgeon.RebuildFreelistOptions{
		SourcePath: src,
		OutputPath: outOneShot,
		StatePath:  filepath.Join(tmp, "one.state"),
	}, -1, 0)
	require.True(t, progOne.Done)

	outBatched := filepath.Join(tmp, "batched.db")
	progBatch := runRebuildLoop(t, surgeon.RebuildFreelistOptions{
		SourcePath: src,
		OutputPath: outBatched,
		StatePath:  filepath.Join(tmp, "batched.state"),
	}, 7, 0)
	require.True(t, progBatch.Done)

	assert.Equal(t, progOne.FreePages, progBatch.FreePages)
	assert.Equal(t, fileHash(t, outOneShot), fileHash(t, outBatched))
}

func TestRebuildFreelist_IdempotentRerun(t *testing.T) {
	src := buildFragmentedDB(t, 4096, true, 500)
	tmp := t.TempDir()
	opts := surgeon.RebuildFreelistOptions{
		SourcePath: src,
		OutputPath: filepath.Join(tmp, "out.db"),
		StatePath:  filepath.Join(tmp, "state.json"),
	}
	prog := runRebuildLoop(t, opts, 5, 0)
	require.True(t, prog.Done)

	// The freelist as persisted in the output must be sorted and unique.
	meta, _, err := guts_cli.GetActiveMetaPage(opts.OutputPath)
	require.NoError(t, err)
	fp, _, err := guts_cli.ReadPage(opts.OutputPath, uint64(meta.Freelist()))
	require.NoError(t, err)
	ids := fp.FreelistPageIds()
	require.Equal(t, prog.FreePages, len(ids))
	require.True(t, sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }))
	seen := make(map[common.Pgid]struct{}, len(ids))
	for _, id := range ids {
		_, dup := seen[id]
		require.False(t, dup, "duplicated freelist entry %d", id)
		seen[id] = struct{}{}
	}

	outputBefore := fileHash(t, opts.OutputPath)

	// Re-running a finished rebuild must be a no-op: no rewrite, same state.
	again, err := surgeon.RunRebuildFreelist(opts)
	require.NoError(t, err)
	assert.True(t, again.Done)
	assert.Equal(t, prog.FreePages, again.FreePages)
	assert.Equal(t, outputBefore, fileHash(t, opts.OutputPath))
}

func TestRebuildFreelist_MismatchInvalidPageTypeStopsAndReportsPage(t *testing.T) {
	src := buildFragmentedDB(t, 4096, true, 400)
	tmp := t.TempDir()

	// Corrupt a *copy*: zero the flags of the root tree page so it is no
	// longer a branch/leaf page.
	corrupt := filepath.Join(tmp, "corrupt.db")
	require.NoError(t, common.CopyFile(src, corrupt))

	pageSize, _, err := guts_cli.ReadPageAndHWMSize(corrupt)
	require.NoError(t, err)
	root, _, err := guts_cli.GetRootPage(corrupt)
	require.NoError(t, err)

	f, err := os.OpenFile(corrupt, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x00, 0x00}, int64(root)*int64(pageSize)+8) // flags field
	require.NoError(t, err)
	require.NoError(t, f.Close())

	before := fileHash(t, corrupt) // baseline after the deliberate corruption

	output := filepath.Join(tmp, "out.db")
	state := filepath.Join(tmp, "state.json")
	opts := surgeon.RebuildFreelistOptions{SourcePath: corrupt, OutputPath: output, StatePath: state, BatchPages: 4}

	_, err = surgeon.RunRebuildFreelist(opts)
	var mismatchErr *surgeon.RebuildMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	found := false
	for _, m := range mismatchErr.Mismatches {
		assert.Contains(t, m.String(), fmt.Sprintf("page %d", root))
		if m.PageID == root && m.Kind == surgeon.MismatchInvalidPageType {
			found = true
		}
	}
	assert.True(t, found, "expected invalid-page-type mismatch for root page %d, got: %v", root, mismatchErr.Mismatches)

	// Nothing must have been written and the corrupted source must be
	// untouched.
	_, err = os.Stat(output)
	assert.True(t, os.IsNotExist(err))
	assert.Equal(t, before, fileHash(t, corrupt))

	// Re-running keeps reporting the same mismatch and changes nothing.
	_, err = surgeon.RunRebuildFreelist(opts)
	require.ErrorAs(t, err, &mismatchErr)
	assert.Equal(t, before, fileHash(t, corrupt))
	_, err = os.Stat(output)
	assert.True(t, os.IsNotExist(err))
}

func TestRebuildFreelist_MismatchReachableFreedStops(t *testing.T) {
	// A synced-freelist database with scattered free pages.
	src := buildFragmentedDB(t, 4096, false, 600)
	tmp := t.TempDir()

	corrupt := filepath.Join(tmp, "corrupt.db")
	require.NoError(t, common.CopyFile(src, corrupt))

	pageSize, _, err := guts_cli.ReadPageAndHWMSize(corrupt)
	require.NoError(t, err)
	root, _, err := guts_cli.GetRootPage(corrupt)
	require.NoError(t, err)
	meta, _, err := guts_cli.GetActiveMetaPage(corrupt)
	require.NoError(t, err)
	require.NotEqual(t, common.PgidNoFreelist, meta.Freelist())

	// Tamper with the persisted freelist: register the live root page as if
	// it were free (inserted in sorted position).
	fp, _, err := guts_cli.ReadPage(corrupt, uint64(meta.Freelist()))
	require.NoError(t, err)
	oldIDs := append([]common.Pgid(nil), fp.FreelistPageIds()...)
	capacity := int((pageSize - uint64(common.PageHeaderSize)) / 8)
	require.Less(t, len(oldIDs)+1, capacity, "test db freelist page must have room for one extra id")
	newIDs := append(oldIDs, root)
	sort.Slice(newIDs, func(i, j int) bool { return newIDs[i] < newIDs[j] })

	nPages := int(fp.Overflow()) + 1
	buf := make([]byte, nPages*int(pageSize))
	p := common.LoadPage(buf)
	p.SetId(meta.Freelist())
	rebuilt := fl2.NewArrayFreelist()
	rebuilt.Init(newIDs)
	rebuilt.Write(p)
	p.SetOverflow(fp.Overflow())
	require.NoError(t, guts_cli.WritePage(corrupt, buf))

	output := filepath.Join(tmp, "out.db")
	opts := surgeon.RebuildFreelistOptions{
		SourcePath: corrupt,
		OutputPath: output,
		StatePath:  filepath.Join(tmp, "state.json"),
		BatchPages: 8,
	}
	_, err = surgeon.RunRebuildFreelist(opts)
	var mismatchErr *surgeon.RebuildMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	found := false
	for _, m := range mismatchErr.Mismatches {
		if m.PageID == root && m.Kind == surgeon.MismatchReachableFreed {
			found = true
		}
	}
	assert.True(t, found, "expected reachable-freed mismatch for root page %d, got: %v", root, mismatchErr.Mismatches)
	_, err = os.Stat(output)
	assert.True(t, os.IsNotExist(err), "no output may be produced when pages disagree")
}

func TestRebuildFreelist_ResumeRejectsChangedSource(t *testing.T) {
	src0 := buildFragmentedDB(t, 4096, true, 300)
	tmp := t.TempDir()

	opts := surgeon.RebuildFreelistOptions{
		SourcePath: src0,
		OutputPath: filepath.Join(tmp, "out.db"),
		StatePath:  filepath.Join(tmp, "state.json"),
		BatchPages: 1,
	}
	partial := runRebuildLoop(t, opts, 1, 2)
	require.False(t, partial.Done)

	// Commit a new transaction on the source so meta txid/hwm change.
	db, err := bolt.Open(src0, 0o600, &bolt.Options{NoFreelistSync: true})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("data")).Put([]byte("key-tail"), []byte("v"))
	}))
	require.NoError(t, db.Close())

	_, err = surgeon.RunRebuildFreelist(opts)
	assert.ErrorIs(t, err, surgeon.ErrRebuildSourceChanged)
}

func TestRebuildFreelist_RejectsInPlaceWrite(t *testing.T) {
	src := buildFragmentedDB(t, 4096, true, 100)
	_, err := surgeon.RunRebuildFreelist(surgeon.RebuildFreelistOptions{
		SourcePath: src,
		OutputPath: src,
		StatePath:  filepath.Join(t.TempDir(), "state.json"),
	})
	assert.ErrorIs(t, err, surgeon.ErrRebuildOutputIsSource)
}
