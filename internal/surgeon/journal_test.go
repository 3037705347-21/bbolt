package surgeon

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

const testPageSize = 4096

func newRepairFixture(t *testing.T, numTx, numKeys int) (srcPath, dstPath string) {
	t.Helper()
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: testPageSize})
	require.NoError(t,
		db.Fill([]byte("data"), numTx, numKeys,
			func(tx int, k int) []byte { return []byte(fmt.Sprintf("%06d", tx*1000000+k)) },
			func(tx int, k int) []byte { return make([]byte, 24) },
		))
	srcPath = db.Path()
	db.MustClose()
	dstPath = filepath.Join(t.TempDir(), "repaired.db")
	return srcPath, dstPath
}

func readRawSpan(t *testing.T, path string, pgid, pageSize, span uint64) []byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, span)
	n, err := f.ReadAt(buf, int64(pgid*pageSize))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	return buf
}

func findLeafPages(t *testing.T, path string, withOverflow bool) []uint64 {
	t.Helper()
	_, hwm, err := guts_cli.ReadPageAndHWMSize(path)
	require.NoError(t, err)
	var ids []uint64
	for id := uint64(2); id < uint64(hwm); id++ {
		p, _, err := guts_cli.ReadPage(path, id)
		if err != nil {
			continue // overflow tails are not self-identifying pages
		}
		if !p.IsLeafPage() {
			continue
		}
		if withOverflow != (p.Overflow() > 0) {
			continue
		}
		ids = append(ids, id)
	}
	require.NotEmpty(t, ids)
	return ids
}

func flipByte(t *testing.T, path string, offset int64) {
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

func emptyLeafAfterImage(pageSize uint64, pgid common.Pgid) []byte {
	buf := make([]byte, pageSize)
	p := common.LoadPage(buf)
	p.SetFlags(common.LeafPageFlag)
	p.SetId(pgid)
	return buf
}

func TestRepairJournal_BeginApplyFinish(t *testing.T) {
	srcPath, dstPath := newRepairFixture(t, 5, 100)
	origData, err := os.ReadFile(srcPath)
	require.NoError(t, err)

	s, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)

	// The header binds the journal to one concrete source version.
	meta, _, err := guts_cli.GetActiveMetaPage(srcPath)
	require.NoError(t, err)
	st := s.Status()
	assert.Equal(t, uint64(meta.Txid()), st.ActiveTxid)
	assert.Equal(t, uint32(testPageSize), st.PageSize)
	assert.False(t, st.Finalized)
	assert.FileExists(t, JournalPath(dstPath))

	// Repair page 2 from page 3; both images must be recorded.
	require.NoError(t, s.CopyPage(3, 2))
	st = s.Status()
	require.Len(t, st.Pages, 1)
	assert.Equal(t, "copy-page", st.Pages[0].Op)
	assert.Equal(t, common.Pgid(2), st.Pages[0].Pgid)
	assert.True(t, st.Pages[0].Committed)
	assert.NotEmpty(t, st.Pages[0].BeforeHash)
	assert.NotEmpty(t, st.Pages[0].AfterHash)
	assert.NotEqual(t, st.Pages[0].BeforeHash, st.Pages[0].AfterHash)
	assert.Equal(t, testPageSize, st.Pages[0].BeforeSize)
	assert.Equal(t, testPageSize, st.Pages[0].AfterSize)
	require.NoError(t, s.Verify())

	// Restore the original page 2 so the final file is structurally intact.
	_, orig2Buf, err := guts_cli.ReadPage(srcPath, 2)
	require.NoError(t, err)
	require.NoError(t, s.ApplyPage("restore-page", 2, orig2Buf))
	require.NoError(t, s.Verify())

	require.NoError(t, s.Finish())
	assert.NoFileExists(t, JournalPath(dstPath))

	finalData, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, origData, finalData)

	// The finished file opens cleanly and passes the consistency check.
	db, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			return err
		}
		return nil
	}))
}

func TestRepairJournal_RollbackUncommitted(t *testing.T) {
	for _, crashStage := range []string{"applyLogged", "pageWritten"} {
		t.Run(crashStage, func(t *testing.T) {
			srcPath, dstPath := newRepairFixture(t, 5, 100)
			origPage2 := readRawSpan(t, srcPath, 2, testPageSize, testPageSize)

			s, err := BeginRepair(srcPath, dstPath)
			require.NoError(t, err)
			s.crashHook = func(stage string) error {
				if stage == crashStage {
					return errSimulatedCrash
				}
				return nil
			}
			err = s.CopyPage(3, 2)
			require.Error(t, err)
			require.ErrorIs(t, err, errSimulatedCrash)
			require.NoError(t, s.Close())

			// Restart: the uncommitted repair must be aborted and the
			// page restored as-is.
			s2, report, err := ResumeRepair(dstPath)
			require.NoError(t, err)
			require.Len(t, report.Aborted, 1)
			assert.Equal(t, common.Pgid(2), report.Aborted[0].Pgid)
			assert.Equal(t, "copy-page", report.Aborted[0].Op)
			if crashStage == "pageWritten" {
				require.Len(t, report.RolledBack, 1)
				assert.Equal(t, common.Pgid(2), report.RolledBack[0].Pgid)
			} else {
				// The disk write never started, so nothing was rewritten.
				assert.Empty(t, report.RolledBack)
			}
			assert.Empty(t, report.Redone)
			assert.Equal(t, origPage2, readRawSpan(t, dstPath, 2, testPageSize, testPageSize))
			assert.Equal(t, StateRolledBack, s2.Status().Pages[0].State)

			// Recovery is idempotent.
			report2, err := s2.Recover()
			require.NoError(t, err)
			assert.Empty(t, report2.RolledBack)
			assert.Empty(t, report2.Redone)
			assert.Empty(t, report2.Aborted)

			// After rollback the same repair can be performed again.
			require.NoError(t, s2.CopyPage(3, 2))
			require.NoError(t, s2.Verify())
			require.NoError(t, s2.Finish())
			assert.NoFileExists(t, JournalPath(dstPath))
		})
	}
}

func TestRepairJournal_RedoCommittedButDamagedPage(t *testing.T) {
	srcPath, dstPath := newRepairFixture(t, 5, 100)

	s, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)
	require.NoError(t, s.CopyPage(3, 2))
	require.NoError(t, s.Close())

	// Post-crash damage on an already committed page.
	flipByte(t, dstPath, int64(2*testPageSize)+100)

	s2, report, err := ResumeRepair(dstPath)
	require.NoError(t, err)
	require.Len(t, report.Redone, 1)
	assert.Equal(t, common.Pgid(2), report.Redone[0].Pgid)
	assert.Empty(t, report.RolledBack)
	require.NoError(t, s2.Verify())
	require.NoError(t, s2.Close())
}

func TestRepairJournal_VerifyDetectsForeignChanges(t *testing.T) {
	srcPath, dstPath := newRepairFixture(t, 5, 100)

	s, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)
	require.NoError(t, s.CopyPage(3, 2))
	require.NoError(t, s.Verify())

	// A change to a repaired page must be detected.
	flipByte(t, dstPath, int64(2*testPageSize)+200)
	err = s.Verify()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "page 2")

	// Recovery fixes the page; afterwards changing a meta page must be
	// detected: the file no longer belongs to the journal's version.
	report, err := s.Recover()
	require.NoError(t, err)
	require.Len(t, report.Redone, 1)
	require.NoError(t, s.Verify())
	require.NoError(t, s.Close())

	require.NoError(t, RevertMetaPage(dstPath))
	s2, _, err := ResumeRepair(dstPath)
	require.NoError(t, err)
	err = s2.Verify()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "meta pages no longer match")
	require.NoError(t, s2.Close())
}

func TestRepairJournal_RollbackRestoresOverflowSpan(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: testPageSize})
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("data"))
		for i := 0; i < 4; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%02d", i)), make([]byte, 5000)); err != nil {
				return err
			}
		}
		return nil
	}))
	srcPath := db.Path()
	db.MustClose()
	dstPath := filepath.Join(t.TempDir(), "repaired.db")

	target := findLeafPages(t, srcPath, true)[0]
	p, _, err := guts_cli.ReadPage(srcPath, target)
	require.NoError(t, err)
	origSpan := testPageSize * (uint64(p.Overflow()) + 1)
	origBytes := readRawSpan(t, srcPath, target, testPageSize, origSpan)

	s, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)
	s.crashHook = func(stage string) error {
		if stage == "pageWritten" {
			return errSimulatedCrash
		}
		return nil
	}
	err = s.ApplyPage("clear-page", common.Pgid(target), emptyLeafAfterImage(testPageSize, common.Pgid(target)))
	require.ErrorIs(t, err, errSimulatedCrash)
	require.NoError(t, s.Close())

	// Rollback must restore the whole original span, overflow tail included.
	s2, report, err := ResumeRepair(dstPath)
	require.NoError(t, err)
	require.Len(t, report.RolledBack, 1)
	require.Len(t, report.Aborted, 1)
	assert.Equal(t, common.Pgid(target), report.RolledBack[0].Pgid) //nolint:gosec
	assert.Equal(t, origBytes, readRawSpan(t, dstPath, target, testPageSize, origSpan))
	require.NoError(t, s2.Verify())
	require.NoError(t, s2.Close())
}

func TestRepairJournal_TornCommitFrameRollsBack(t *testing.T) {
	srcPath, dstPath := newRepairFixture(t, 5, 100)
	origPage2 := readRawSpan(t, srcPath, 2, testPageSize, testPageSize)

	s, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)
	require.NoError(t, s.CopyPage(3, 2))
	require.NoError(t, s.Close())

	// Simulate a crash while the COMMIT frame was being appended: the
	// frame is truncated, so the parser ignores it and recovery treats
	// the page as uncommitted and rolls it back.
	jpath := JournalPath(dstPath)
	raw, err := os.ReadFile(jpath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jpath, raw[:len(raw)-7], 0600))

	s2, report, err := ResumeRepair(dstPath)
	require.NoError(t, err)
	require.Len(t, report.RolledBack, 1)
	require.Len(t, report.Aborted, 1)
	assert.Equal(t, origPage2, readRawSpan(t, dstPath, 2, testPageSize, testPageSize))
	// All pages are either committed or rolled back, so the repair can
	// be finalized without leaving any half-modified page behind.
	require.NoError(t, s2.Finish())
}

func TestRepairJournal_RejectsMismatchedAfterImage(t *testing.T) {
	srcPath, dstPath := newRepairFixture(t, 3, 50)

	s, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)
	defer s.Close()

	_, buf, err := guts_cli.ReadPage(dstPath, 3)
	require.NoError(t, err)
	common.LoadPage(buf).SetId(99)
	err = s.ApplyPage("copy-page", 2, buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after-image page id")

	// A rejected apply must not have recorded anything.
	assert.Empty(t, s.Status().Pages)
}

func TestRepairJournal_ResumeWithoutJournalFails(t *testing.T) {
	srcPath, _ := newRepairFixture(t, 1, 10)
	_, _, err := ResumeRepair(srcPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no repair journal found")
}

func TestRepairJournal_BeginRejectsExistingOutput(t *testing.T) {
	srcPath, dstPath := newRepairFixture(t, 1, 10)
	s1, err := BeginRepair(srcPath, dstPath)
	require.NoError(t, err)
	require.NoError(t, s1.Close())

	// The output file and its journal both exist: resume must be used
	// instead of starting a new repair.
	_, err = BeginRepair(srcPath, dstPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestRepairJournal_BeginRejectsStaleJournalWithoutDB(t *testing.T) {
	srcPath, _ := newRepairFixture(t, 1, 10)
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "repaired.db")
	require.NoError(t, os.WriteFile(JournalPath(dstPath), []byte("stale"), 0600))

	_, err := BeginRepair(srcPath, dstPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repair journal already exists")
}
