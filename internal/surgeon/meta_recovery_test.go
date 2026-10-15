package surgeon_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/surgeon"
)

const (
	// Byte offsets inside a meta page: 16B page header; checksum is the
	// last 8-byte field of the 64B meta struct (offset 56).
	metaMagicOff    = 16
	metaChecksumOff = 16 + 56
	metaRootOff     = 16 + 16
)

// createDBWithTwoVersions produces a closed DB whose bucket "data" ends up
// with foo=baz; the previous transaction had foo=bar.
func createDBWithTwoVersions(t *testing.T) *btesting.DB {
	t.Helper()
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
	return db
}

func healthyPageSize(t *testing.T, path string) uint32 {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, 1024)
	n, err := f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 80)
	return common.LoadPageMeta(buf).PageSize()
}

func readRawPage(t *testing.T, path string, id uint64, pageSize uint32) []byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, pageSize)
	n, err := f.ReadAt(buf, int64(id)*int64(pageSize))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	return buf
}

func writeRawPage(t *testing.T, path string, id uint64, pageSize uint32, buf []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer f.Close()
	n, err := f.WriteAt(buf, int64(id)*int64(pageSize))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.NoError(t, f.Sync())
}

func flipByteAt(t *testing.T, path string, off int64) {
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

func copyFileBytes(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	require.NoError(t, err)
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	defer out.Close()
	_, err = io.Copy(out, in)
	require.NoError(t, err)
	require.NoError(t, out.Sync())
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

func pathExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	require.True(t, os.IsNotExist(err), "unexpected stat error: %v", err)
	return false
}

func mustCheckDB(t *testing.T, path string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var errs []error
		for err := range tx.Check() {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}))
}

func readFoo(t *testing.T, path string) string {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	defer db.Close()
	var v string
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		v = string(tx.Bucket([]byte("data")).Get([]byte("foo")))
		return nil
	}))
	return v
}

// makeTornActiveMeta copies the healthy DB and flips a checksum byte on the
// most recent (active) meta page, simulating a torn meta write. Returns the
// corrupted path, page size and the active/stale page positions.
func makeTornActiveMeta(t *testing.T, dir, srcPath string) (string, uint32, uint64, uint64) {
	t.Helper()
	pageSize := healthyPageSize(t, srcPath)
	_, insp, err := surgeon.InspectMetaPages(srcPath)
	require.NoError(t, err)
	var active, stale uint64
	if insp[0].Txid >= insp[1].Txid {
		active, stale = 0, 1
	} else {
		active, stale = 1, 0
	}
	tornPath := filepath.Join(dir, "torn.db")
	copyFileBytes(t, srcPath, tornPath)
	flipByteAt(t, tornPath, int64(active)*int64(pageSize)+metaChecksumOff)
	return tornPath, pageSize, active, stale
}

func makeBothMetaInvalid(t *testing.T, dir, srcPath string) (string, uint32) {
	t.Helper()
	pageSize := healthyPageSize(t, srcPath)
	bad := filepath.Join(dir, "bothbad.db")
	copyFileBytes(t, srcPath, bad)
	flipByteAt(t, bad, metaChecksumOff)
	flipByteAt(t, bad, int64(pageSize)+metaChecksumOff)
	return bad, pageSize
}

// makeSameTxidDivergence places two checksum-valid metas with identical
// txid but different contents at positions 0 and 1. mutate selects which
// position receives the modified (rootSequence+1) meta.
func makeSameTxidDivergence(t *testing.T, dir, srcPath string, identical bool) string {
	t.Helper()
	pageSize := healthyPageSize(t, srcPath)
	buf0 := readRawPage(t, srcPath, 0, pageSize)
	outPath := filepath.Join(dir, "divergent.db")
	copyFileBytes(t, srcPath, outPath)

	page0 := append([]byte(nil), buf0...)
	page1 := append([]byte(nil), buf0...)
	common.LoadPage(page1).SetId(1)
	if !identical {
		m := common.LoadPageMeta(page1)
		m.RootBucket().SetInSequence(m.RootBucket().InSequence() + 1)
		m.SetChecksum(m.Sum64())
	}
	writeRawPage(t, outPath, 0, pageSize, page0)
	writeRawPage(t, outPath, 1, pageSize, page1)
	return outPath
}

func runRecover(t *testing.T, src, out, journal string, opts surgeon.RecoverOptions) *surgeon.RecoveryReport {
	t.Helper()
	report, err := surgeon.RecoverMetaPages(src, out, journal, opts)
	require.NoError(t, err)
	return report
}

func loadJournalMap(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var j map[string]any
	require.NoError(t, json.Unmarshal(data, &j))
	return j
}

func currentEpisode(t *testing.T, j map[string]any) map[string]any {
	t.Helper()
	cur, ok := j["current"].(map[string]any)
	require.True(t, ok, "journal has no current episode: %v", j)
	return cur
}

func journalEvents(t *testing.T, cur map[string]any) []map[string]any {
	t.Helper()
	rawEvents, ok := cur["events"].([]any)
	require.True(t, ok)
	events := make([]map[string]any, 0, len(rawEvents))
	for _, e := range rawEvents {
		em, ok := e.(map[string]any)
		require.True(t, ok)
		events = append(events, em)
	}
	return events
}

// setJournalStatus rewinds the persisted status to simulate an interrupted
// run; phase events that can only exist after the target status are removed
// to keep the fixture realistic.
func setJournalStatus(t *testing.T, path, status string) {
	t.Helper()
	j := loadJournalMap(t, path)
	cur := currentEpisode(t, j)
	cur["status"] = status
	if status == "decided" {
		kept := make([]any, 0)
		for _, e := range cur["events"].([]any) {
			em := e.(map[string]any)
			msg := em["message"].(string)
			if containsPrefix(msg, "applied") || containsPrefix(msg, "completed") ||
				(em["type"] != "rollback" && containsPrefix(msg, "resumed")) {
				continue
			}
			kept = append(kept, e)
		}
		cur["events"] = kept
		delete(cur, "completedAt")
	}
	data, err := json.MarshalIndent(j, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func containsPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func TestInspectMetaPagesHealthy(t *testing.T) {
	db := createDBWithTwoVersions(t)
	pageSize, insp, err := surgeon.InspectMetaPages(db.Path())
	require.NoError(t, err)
	assert.NotZero(t, pageSize)
	for i := 0; i < 2; i++ {
		assert.True(t, insp[i].Readable)
		assert.Empty(t, insp[i].ValidateError)
		assert.True(t, insp[i].MagicOK && insp[i].VersionOK && insp[i].ChecksumOK)
		assert.Len(t, insp[i].Fingerprint, 64)
		assert.True(t, insp[i].StructuralOK)
		assert.NotZero(t, insp[i].Txid)
	}
	assert.NotEqual(t, insp[0].Txid, insp[1].Txid)
}

func TestInspectMetaPagesPageSizeProbeFromMeta1(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	broken0 := filepath.Join(dir, "broken0.db")
	copyFileBytes(t, db.Path(), broken0)
	pageSize := healthyPageSize(t, broken0)
	// Destroy meta0 magic; meta1 stays intact, so page size must be probed
	// from meta1.
	flipByteAt(t, broken0, metaMagicOff)

	got, insp, err := surgeon.InspectMetaPages(broken0)
	require.NoError(t, err)
	assert.Equal(t, pageSize, got)
	assert.False(t, insp[0].MagicOK)
	assert.NotEmpty(t, insp[0].ValidateError)
	assert.True(t, insp[1].Valid())
}

func TestInspectMetaPagesBothMagicBroken(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	broken := filepath.Join(dir, "broken.db")
	copyFileBytes(t, db.Path(), broken)
	pageSize := healthyPageSize(t, broken)
	flipByteAt(t, broken, metaMagicOff)
	flipByteAt(t, broken, int64(pageSize)+metaMagicOff)

	before := fileSHA(t, broken)
	got, insp, err := surgeon.InspectMetaPages(broken)
	require.NoError(t, err)
	assert.Zero(t, got)
	assert.False(t, insp[0].Valid())
	assert.False(t, insp[1].Valid())
	assert.Equal(t, before, fileSHA(t, broken), "inspection must be read-only")
}

func TestRecoverTornNewerMetaPage(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	torn, _, active, stale := makeTornActiveMeta(t, dir, db.Path())

	out := filepath.Join(dir, "recovered.db")
	journal := filepath.Join(dir, "journal.json")
	report := runRecover(t, torn, out, journal, surgeon.RecoverOptions{})

	assert.Equal(t, int(stale), report.SelectedPage)
	assert.Equal(t, surgeon.ReasonOnlyValid, report.Reason)
	assert.Equal(t, surgeon.ActionCopyPage, report.Action.Type)
	require.NotNil(t, report.Rollback, "discarding the torn higher txid must be recorded as rollback")
	assert.Equal(t, int(active), report.Rollback.DiscardedPage)

	mustCheckDB(t, out)
	assert.Equal(t, "bar", readFoo(t, out), "recovered state must match the valid older txid")

	j := loadJournalMap(t, journal)
	cur := currentEpisode(t, j)
	assert.Equal(t, "completed", cur["status"])
	assert.Equal(t, journal, report.JournalPath)
	dec := cur["decision"].(map[string]any)
	assert.EqualValues(t, stale, dec["selectedPage"])
	assert.Equal(t, surgeon.ReasonOnlyValid, dec["reason"])
	assert.Len(t, cur["backups"].([]any), 2)
	inspections := cur["inspections"].([]any)
	for _, raw := range inspections {
		fp := raw.(map[string]any)["fingerprint"].(string)
		assert.Len(t, fp, 64)
	}
	var sawRollback bool
	for _, e := range journalEvents(t, cur) {
		if e["type"] == "rollback" {
			sawRollback = true
			assert.Contains(t, e["message"].(string), "rollback: discard meta page")
		}
	}
	assert.True(t, sawRollback)

	// --select-meta-page must be rejected when re-running an episode that
	// was not ambiguous (review finding F1): the recorded decision stays
	// authoritative and the flag must not be silently accepted.
	forcedStale := int(stale)
	_, err := surgeon.RecoverMetaPages(torn, out, journal, surgeon.RecoverOptions{ForcedPage: &forcedStale})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only allowed when both meta pages are valid with the same txid")
}

func TestRecoverBothMetaPagesInvalidTouchesNothing(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	bad, _ := makeBothMetaInvalid(t, dir, db.Path())
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")
	before := fileSHA(t, bad)

	_, err := surgeon.RecoverMetaPages(bad, out, journal, surgeon.RecoverOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, surgeon.ErrBothMetaPagesInvalid), err)
	assert.Contains(t, err.Error(), "page 0")
	assert.Contains(t, err.Error(), "page 1")

	assert.Equal(t, before, fileSHA(t, bad), "source must be untouched")
	assert.False(t, pathExists(t, out), "no output file may be created")

	j := loadJournalMap(t, journal)
	cur := currentEpisode(t, j)
	assert.Equal(t, "aborted", cur["status"])
	assert.Contains(t, cur["abortError"].(string), surgeon.ErrBothMetaPagesInvalid.Error())
}

func TestRecoverHealthyPicksHigherTxid(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")

	report := runRecover(t, db.Path(), out, journal, surgeon.RecoverOptions{})
	assert.Equal(t, surgeon.ReasonHigherTxid, report.Reason)
	mustCheckDB(t, out)
	assert.Equal(t, "baz", readFoo(t, out))

	cur := currentEpisode(t, loadJournalMap(t, journal))
	assert.Equal(t, "completed", cur["status"])
	dec := cur["decision"].(map[string]any)
	assert.Equal(t, surgeon.ReasonHigherTxid, dec["reason"])
	post := cur["expectedPostFingerprints"].([]any)
	assert.Equal(t, post[0], post[1])
}

func TestRecoverIdenticalIsByteStable(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	src := makeSameTxidDivergence(t, dir, db.Path(), true)
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")

	report := runRecover(t, src, out, journal, surgeon.RecoverOptions{})
	assert.Equal(t, surgeon.ReasonIdentical, report.Reason)
	assert.Equal(t, surgeon.ActionNone, report.Action.Type)
	assert.Equal(t, fileSHA(t, src), fileSHA(t, out))
	assert.Empty(t, report.BackupPaths)
}

func TestRecoverAmbiguousWithoutForceAborts(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	src := makeSameTxidDivergence(t, dir, db.Path(), false)
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")

	_, err := surgeon.RecoverMetaPages(src, out, journal, surgeon.RecoverOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, surgeon.ErrMetaPagesAmbiguous), err)
	assert.Contains(t, err.Error(), "rootSequence")
	assert.False(t, pathExists(t, out))
}

func TestRecoverAmbiguousWithExplicitSelection(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	src := makeSameTxidDivergence(t, dir, db.Path(), false)
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")

	_, err := surgeon.RecoverMetaPages(src, out, journal, surgeon.RecoverOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, surgeon.ErrMetaPagesAmbiguous))

	forced := 0
	report := runRecover(t, src, out, journal, surgeon.RecoverOptions{ForcedPage: &forced})
	assert.Equal(t, 0, report.SelectedPage)
	assert.Equal(t, surgeon.ReasonOperatorForced, report.Reason)
	mustCheckDB(t, out)

	cur := currentEpisode(t, loadJournalMap(t, journal))
	assert.Equal(t, surgeon.ReasonOperatorForced, cur["decision"].(map[string]any)["reason"])
	diverging := cur["decision"].(map[string]any)["divergingFields"].([]any)
	assert.NotEmpty(t, diverging)

	// Re-running without repeating --select-meta-page must reuse the
	// recorded operator decision instead of aborting as ambiguous again.
	report, err = surgeon.RecoverMetaPages(src, out, journal, surgeon.RecoverOptions{})
	require.NoError(t, err)
	assert.True(t, report.AlreadyConsistent)
	assert.Equal(t, surgeon.ReasonOperatorForced, report.Reason)

	// A conflicting explicit selection is rejected.
	other := 1
	_, err = surgeon.RecoverMetaPages(src, out, journal, surgeon.RecoverOptions{ForcedPage: &other})
	require.Error(t, err)
	assert.True(t, errors.Is(err, surgeon.ErrRecoveryStateMismatch))
}

func TestRecoverForcedPageRejectedWhenNotAmbiguous(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")
	forced := 0
	_, err := surgeon.RecoverMetaPages(db.Path(), out, journal, surgeon.RecoverOptions{ForcedPage: &forced})
	require.Error(t, err)
	assert.False(t, pathExists(t, out))
}

func TestRecoverInterruptedResumeScenarios(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	torn, _, _, _ := makeTornActiveMeta(t, dir, db.Path())
	out := filepath.Join(dir, "recovered.db")
	journal := filepath.Join(dir, "journal.json")

	// Initial completed run establishes the reference output.
	report := runRecover(t, torn, out, journal, surgeon.RecoverOptions{})
	referenceHash := fileSHA(t, out)
	require.Equal(t, surgeon.ActionCopyPage, report.Action.Type)

	// 1. Journal says decided and output is the untouched pre-repair copy.
	require.NoError(t, os.Remove(out))
	copyFileBytes(t, torn, out)
	setJournalStatus(t, journal, "decided")
	report, err := surgeon.RecoverMetaPages(torn, out, journal, surgeon.RecoverOptions{})
	require.NoError(t, err)
	assert.True(t, report.Resumed)
	assert.Equal(t, referenceHash, fileSHA(t, out), "resumed repair must produce the same bytes")
	mustCheckDB(t, out)

	// 2. Journal rewound to decided but output is already post-repair:
	//    no write may happen.
	setJournalStatus(t, journal, "decided")
	before := fileSHA(t, out)
	report, err = surgeon.RecoverMetaPages(torn, out, journal, surgeon.RecoverOptions{})
	require.NoError(t, err)
	assert.True(t, report.Resumed)
	assert.True(t, report.AlreadyConsistent)
	assert.Equal(t, before, fileSHA(t, out))

	// 3. Completed journal but output deleted: rebuild from source.
	require.NoError(t, os.Remove(out))
	report, err = surgeon.RecoverMetaPages(torn, out, journal, surgeon.RecoverOptions{})
	require.NoError(t, err)
	assert.True(t, report.Resumed)
	assert.Equal(t, referenceHash, fileSHA(t, out))

	// 4. Plain rerun after completion is a no-op success.
	before = fileSHA(t, out)
	report, err = surgeon.RecoverMetaPages(torn, out, journal, surgeon.RecoverOptions{})
	require.NoError(t, err)
	assert.True(t, report.AlreadyConsistent)
	assert.Equal(t, before, fileSHA(t, out))
}

func TestRecoverStateMismatchRefusesToFlipFlop(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	torn, pageSize, _, _ := makeTornActiveMeta(t, dir, db.Path())
	out := filepath.Join(dir, "recovered.db")
	journal := filepath.Join(dir, "journal.json")
	runRecover(t, torn, out, journal, surgeon.RecoverOptions{})

	// Tamper with the completed output: its fingerprints now match neither
	// recorded state.
	flipByteAt(t, out, metaRootOff+int64(pageSize))
	before := fileSHA(t, out)
	_, err := surgeon.RecoverMetaPages(torn, out, journal, surgeon.RecoverOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, surgeon.ErrRecoveryStateMismatch), err)
	assert.Equal(t, before, fileSHA(t, out), "mismatched output must not be modified")

	// The refusal itself must be recorded as an error event in the journal.
	cur := currentEpisode(t, loadJournalMap(t, journal))
	var sawMismatchEvent bool
	for _, e := range journalEvents(t, cur) {
		if e["type"] == "error" && strings.Contains(e["message"].(string), "match neither") {
			sawMismatchEvent = true
		}
	}
	assert.True(t, sawMismatchEvent, "journal must record the state-mismatch refusal")
}

func TestRecoverBackupsMatchOriginalMetaPages(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	torn, pageSize, _, _ := makeTornActiveMeta(t, dir, db.Path())
	out := filepath.Join(dir, "recovered.db")
	journal := filepath.Join(dir, "journal.json")
	report := runRecover(t, torn, out, journal, surgeon.RecoverOptions{})
	require.Len(t, report.BackupPaths, 2)

	originals := map[uint64][]byte{
		0: readRawPage(t, torn, 0, pageSize),
		1: readRawPage(t, torn, 1, pageSize),
	}
	for _, backup := range report.BackupPaths {
		var id uint64
		rest := filepath.Base(backup)
		_, err := fmt.Sscanf(rest, "recovered.db.meta-page-%d.", &id)
		require.NoError(t, err)
		data, err := os.ReadFile(backup)
		require.NoError(t, err)
		assert.Equal(t, originals[id], data, "backup of page %d must equal the pre-repair page", id)
	}
}

func TestRecoverDeterministicAcrossIndependentRuns(t *testing.T) {
	db := createDBWithTwoVersions(t)
	torn, _, _, _ := makeTornActiveMeta(t, t.TempDir(), db.Path())

	var firstHash string
	var firstReport *surgeon.RecoveryReport
	for i := 0; i < 3; i++ {
		dir := t.TempDir()
		out := filepath.Join(dir, "out.db")
		journal := filepath.Join(dir, "journal.json")
		report := runRecover(t, torn, out, journal, surgeon.RecoverOptions{})
		hash := fileSHA(t, out)
		if i == 0 {
			firstHash = hash
			firstReport = report
		} else {
			assert.Equal(t, firstHash, hash)
			assert.Equal(t, firstReport.SelectedPage, report.SelectedPage)
			assert.Equal(t, firstReport.Reason, report.Reason)
			require.NotNil(t, report.Rollback)
		}
		mustCheckDB(t, out)
	}
}

func TestRecoverRotatesEpisodeWhenSourceChanged(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")
	runRecover(t, db.Path(), out, journal, surgeon.RecoverOptions{})

	// Source database advances by another transaction.
	reopened, err := bolt.Open(db.Path(), 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, reopened.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("data")).Put([]byte("foo"), []byte("qux"))
	}))
	require.NoError(t, reopened.Close())

	require.NoError(t, os.Remove(out))
	runRecover(t, db.Path(), out, journal, surgeon.RecoverOptions{})

	j := loadJournalMap(t, journal)
	assert.Len(t, j["history"].([]any), 1, "previous episode must be preserved verbatim")
	cur := currentEpisode(t, j)
	assert.Equal(t, "completed", cur["status"])
	assert.Equal(t, "qux", readFoo(t, out))
}

func TestRecoverRefusesCorruptJournal(t *testing.T) {
	db := createDBWithTwoVersions(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "out.db")
	journal := filepath.Join(dir, "journal.json")
	require.NoError(t, os.WriteFile(journal, []byte("{this is not json"), 0o600))

	_, err := surgeon.RecoverMetaPages(db.Path(), out, journal, surgeon.RecoverOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse recovery journal")
	data, err := os.ReadFile(journal)
	require.NoError(t, err)
	assert.Equal(t, "{this is not json", string(data), "corrupt journal must not be overwritten")
	assert.False(t, pathExists(t, out))
}
