package surgeon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt/internal/common"
)

// RecoverOptions controls a meta page recovery run.
type RecoverOptions struct {
	// ForcedPage must be nil unless the pages are ambiguous. When set it
	// must point to the physical meta page id (0 or 1) the operator
	// explicitly chooses.
	ForcedPage *int
}

// RecoveryReport is the human-readable outcome of a recovery run.
type RecoveryReport struct {
	SelectedPage int
	SelectedTxid uint64
	Reason       string
	Action       RecoveryAction
	Rollback     *RollbackInfo

	OutputPath  string
	JournalPath string
	BackupPaths []string

	// Resumed is true when an existing journal episode drove the run.
	Resumed bool
	// AlreadyConsistent is true when the output was already in the
	// expected post-recovery state and no write was performed.
	AlreadyConsistent bool
}

// RecoverMetaPages recovers diverged meta pages of srcPath into a fresh
// copy at outPath and records the decision trail at journalPath.
//
// The source file is opened read-only and is never modified. When both
// meta pages are invalid (or the pages are ambiguous without an explicit
// selection), the function returns an error, creates no output file and
// writes no meta bytes. Re-running with the same source and journal always
// converges to the same recorded conclusion.
func RecoverMetaPages(srcPath, outPath, journalPath string, opts RecoverOptions) (*RecoveryReport, error) {
	if srcPath == "" || outPath == "" || journalPath == "" {
		return nil, fmt.Errorf("source, output and journal paths must all be non-empty")
	}

	fi, err := os.Stat(srcPath)
	if err != nil {
		return nil, fmt.Errorf("stat source database %q: %w", srcPath, err)
	}
	sourceSize := fi.Size()

	pageSize, inspections, err := inspectMetaPages(srcPath)
	if err != nil {
		return nil, err
	}
	preFingerprints := fingerprintsOf(inspections)

	journal, _, err := loadJournal(journalPath)
	if err != nil {
		return nil, err
	}

	// A live episode for exactly this source state is authoritative: the
	// conclusion was already decided, so resume from it without asking the
	// policy again. In particular an operator-forced decision must remain
	// usable even when --select-meta-page is not repeated on re-run.
	if journal.Current != nil &&
		journal.Current.Status != statusAborted &&
		journal.Current.matchesSource(sourceSize, preFingerprints) {
		if opts.ForcedPage != nil {
			// The override is only meaningful for an episode that was
			// itself ambiguous and operator-resolved.
			if journal.Current.Decision.Reason != ReasonOperatorForced {
				return nil, fmt.Errorf("--select-meta-page is only allowed when both meta pages are valid with the same txid but different contents; the recorded decision is %q", journal.Current.Decision.Reason)
			}
			if *opts.ForcedPage != journal.Current.Decision.SelectedPage {
				return nil, fmt.Errorf("%w: journal %q records selected meta page %d but --select-meta-page=%d was given",
					ErrRecoveryStateMismatch, journalPath, journal.Current.Decision.SelectedPage, *opts.ForcedPage)
			}
		}
		return resumeEpisode(srcPath, outPath, journalPath, journal, pageSize, preFingerprints)
	}

	decision, decideErr := DecideMetaRecovery(inspections, nil)
	if decideErr != nil {
		if IsAmbiguous(decideErr) && opts.ForcedPage != nil {
			decision, decideErr = DecideMetaRecovery(inspections, opts.ForcedPage)
		}
	} else if opts.ForcedPage != nil {
		// Never allow the operator override to mask an automatic decision.
		return nil, fmt.Errorf("--select-meta-page is only allowed when both meta pages are valid with the same txid but different contents; current decision is %q", decision.Reason)
	}
	if decideErr != nil {
		recordAbortedEpisode(srcPath, journalPath, sourceSize, pageSize, preFingerprints, inspections, decideErr)
		return nil, decideErr
	}

	return runNewEpisode(srcPath, outPath, journalPath, journal, sourceSize, pageSize, inspections, preFingerprints, decision)
}

func runNewEpisode(srcPath, outPath, journalPath string, journal *recoveryJournal, sourceSize int64, pageSize uint32, inspections [metaPageCount]MetaPageInspection, preFingerprints [metaPageCount]string, decision Decision) (*RecoveryReport, error) {
	if _, err := os.Stat(outPath); err == nil {
		return nil, fmt.Errorf("output file %q already exists; refuse to overwrite it", outPath)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat output file %q: %w", outPath, err)
	}

	ep := &journalEpisode{
		StartedAt:                nowRFC3339(),
		Status:                   statusDecided,
		SourcePath:               srcPath,
		OutputPath:               outPath,
		SourceSize:               sourceSize,
		PageSize:                 pageSize,
		PreFingerprints:          preFingerprints,
		ExpectedPostFingerprints: expectedPostFingerprints(decision, preFingerprints),
		Inspections:              inspections,
		Decision:                 decision,
	}
	ep.appendEvent(eventPhase, fmt.Sprintf("decided: select meta page %d, reason %q, action %s", decision.SelectedPage, decision.Reason, describeAction(decision.Action)))
	if decision.Rollback != nil {
		ep.appendEvent(eventRollback, formatRollback(decision.Rollback))
	}
	journal.beginEpisode(ep)

	// Persist the decision before touching anything else, so an
	// interrupted run always knows the exact intended action.
	if err := saveJournalAtomic(journalPath, journal); err != nil {
		return nil, err
	}

	if err := common.CopyFile(srcPath, outPath); err != nil {
		return nil, fmt.Errorf("copy source database to output: %w", err)
	}
	ep.appendEvent(eventInfo, fmt.Sprintf("copied source database to output %q", outPath))

	if decision.Action.Type == ActionCopyPage {
		if err := executeRepair(outPath, journalPath, pageSize, decision, ep); err != nil {
			return nil, err
		}
		if err := stageTransition(journal, journalPath, statusApplied, fmt.Sprintf("applied: %s", describeAction(decision.Action))); err != nil {
			return nil, err
		}
	}

	if err := verifyOutput(outPath, pageSize, ep.ExpectedPostFingerprints); err != nil {
		ep.appendEvent(eventError, err.Error())
		_ = saveJournalAtomic(journalPath, journal)
		return nil, err
	}

	if err := stageTransition(journal, journalPath, statusCompleted, "completed: output meta pages match the recorded decision"); err != nil {
		return nil, err
	}

	return buildReport(ep, decision, journalPath, false, false), nil
}

func resumeEpisode(srcPath, outPath, journalPath string, journal *recoveryJournal, pageSize uint32, preFingerprints [metaPageCount]string) (*RecoveryReport, error) {
	ep := journal.Current
	decision := ep.Decision

	_, err := os.Stat(outPath)
	outputExists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat output file %q: %w", outPath, err)
	}

	if !outputExists {
		// The output disappeared (interrupted copy, or deleted after
		// completion). Rebuild it from the unchanged source and apply the
		// same recorded action.
		if err := common.CopyFile(srcPath, outPath); err != nil {
			return nil, fmt.Errorf("re-create output database: %w", err)
		}
		ep.appendEvent(eventInfo, "resumed: output was missing, copied the unchanged source again")
		if decision.Action.Type == ActionCopyPage {
			if err := executeRepair(outPath, journalPath, pageSize, decision, ep); err != nil {
				return nil, err
			}
			if err := stageTransition(journal, journalPath, statusApplied, fmt.Sprintf("resumed: applied recorded action %s", describeAction(decision.Action))); err != nil {
				return nil, err
			}
		}
		if err := verifyOutput(outPath, pageSize, ep.ExpectedPostFingerprints); err != nil {
			ep.appendEvent(eventError, err.Error())
			_ = saveJournalAtomic(journalPath, journal)
			return nil, err
		}
		if err := stageTransition(journal, journalPath, statusCompleted, "resumed: output rebuilt and matches the recorded decision"); err != nil {
			return nil, err
		}
		return buildReport(ep, decision, journalPath, true, false), nil
	}

	outPageSize, outInspections, inspectErr := inspectMetaPages(outPath)
	if inspectErr != nil {
		return nil, fmt.Errorf("inspect existing output %q: %w", outPath, inspectErr)
	}
	if outPageSize != 0 && outPageSize != pageSize {
		return nil, fmt.Errorf("%w: output page size %d differs from the recorded %d", ErrRecoveryStateMismatch, outPageSize, pageSize)
	}
	outFingerprints := fingerprintsOf(outInspections)

	switch {
	case outFingerprints == ep.ExpectedPostFingerprints:
		// Recovery was already applied (or never needed). Do not write
		// anything; just make sure the journal reflects completion.
		if ep.Status != statusCompleted {
			if decision.Action.Type == ActionCopyPage && ep.Status == statusDecided {
				ep.appendEvent(eventPhase, fmt.Sprintf("resumed: recorded action %s was already applied", describeAction(decision.Action)))
			}
			if err := stageTransition(journal, journalPath, statusCompleted, "resumed: output already matches the recorded decision"); err != nil {
				return nil, err
			}
		}
		return buildReport(ep, decision, journalPath, true, true), nil

	case outFingerprints == preFingerprints:
		// Output is the untouched pre-repair copy: apply the recorded
		// action now.
		ep.appendEvent(eventInfo, "resumed: output was still in the pre-recovery state")
		if decision.Action.Type == ActionCopyPage {
			if err := executeRepair(outPath, journalPath, pageSize, decision, ep); err != nil {
				return nil, err
			}
			if err := stageTransition(journal, journalPath, statusApplied, fmt.Sprintf("resumed: applied recorded action %s", describeAction(decision.Action))); err != nil {
				return nil, err
			}
		}
		if err := verifyOutput(outPath, pageSize, ep.ExpectedPostFingerprints); err != nil {
			ep.appendEvent(eventError, err.Error())
			_ = saveJournalAtomic(journalPath, journal)
			return nil, err
		}
		if err := stageTransition(journal, journalPath, statusCompleted, "resumed: output now matches the recorded decision"); err != nil {
			return nil, err
		}
		return buildReport(ep, decision, journalPath, true, false), nil

	default:
		msg := fmt.Sprintf("resumed: output meta fingerprints %v match neither the recorded pre-repair %v nor post-repair %v state; refusing to take any action",
			outFingerprints, preFingerprints, ep.ExpectedPostFingerprints)
		ep.appendEvent(eventError, msg)
		_ = saveJournalAtomic(journalPath, journal)
		return nil, fmt.Errorf("%w: %s", ErrRecoveryStateMismatch, msg)
	}
}

// executeRepair backs up both original meta pages of the output and then
// applies the single recorded copy-page action.
func executeRepair(outPath, journalPath string, pageSize uint32, decision Decision, ep *journalEpisode) error {
	pages, err := readRawMetaPages(outPath, pageSize)
	if err != nil {
		return err
	}

	backupPaths, err := backupMetaPages(filepath.Dir(journalPath), outPath, pages)
	if err != nil {
		return err
	}
	for _, p := range backupPaths {
		if !containsString(ep.Backups, p) {
			ep.Backups = append(ep.Backups, p)
		}
	}
	ep.appendEvent(eventInfo, fmt.Sprintf("backed up both original meta pages to %v", backupPaths))

	src := decision.Action.From
	dst := decision.Action.To
	buf := make([]byte, len(pages[src]))
	copy(buf, pages[src])
	// The copied meta keeps its 64-byte meta region (and checksum); only
	// the page header id is rewritten to its new physical position.
	common.LoadPage(buf).SetId(common.Pgid(dst))

	f, err := os.OpenFile(outPath, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open output for meta repair: %w", err)
	}
	offset := int64(dst) * int64(pageSize)
	n, err := f.WriteAt(buf, offset)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write repaired meta page %d to output: %w", dst, err)
	}
	ep.appendEvent(eventInfo, fmt.Sprintf("copied meta page %d over page %d (page header id rewritten to %d)", src, dst, dst))
	return nil
}

// backupMetaPages persists a copy of both original meta pages in the
// journal directory, so the pre-repair versions can never be lost.
func backupMetaPages(journalDir, outPath string, pages [metaPageCount][]byte) ([]string, error) {
	base := filepath.Base(outPath)
	paths := make([]string, 0, metaPageCount)
	for id := 0; id < metaPageCount; id++ {
		fp := metaFingerprint(pages[id])
		name := fmt.Sprintf("%s.meta-page-%d.%s.bak", base, id, fp[:8])
		backupPath := filepath.Join(journalDir, name)
		paths = append(paths, backupPath)

		if existing, err := os.ReadFile(backupPath); err == nil {
			// A previous attempt already kept a backup. The name is
			// content-addressed by the meta fingerprint, but verify the
			// full bytes instead of trusting an 8-hex-digit prefix.
			if !bytes.Equal(existing, pages[id]) {
				return nil, fmt.Errorf("meta page backup %q already exists with different content; refusing to overwrite", backupPath)
			}
			continue
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read meta page backup %q: %w", backupPath, err)
		}
		if err := writeFileAtomic(backupPath, pages[id]); err != nil {
			return nil, fmt.Errorf("back up meta page %d: %w", id, err)
		}
	}
	return paths, nil
}

func verifyOutput(outPath string, pageSize uint32, expected [metaPageCount]string) error {
	gotSize, got, err := inspectMetaPages(outPath)
	if err != nil {
		return fmt.Errorf("re-inspect output after repair: %w", err)
	}
	if gotSize != pageSize {
		return fmt.Errorf("output page size changed after repair: %d != %d", gotSize, pageSize)
	}
	for id := 0; id < metaPageCount; id++ {
		if !got[id].Valid() {
			return fmt.Errorf("repaired meta page %d fails validation: %s", id, pageFailure(got[id]))
		}
		if got[id].Fingerprint != expected[id] {
			return fmt.Errorf("repaired meta page %d fingerprint %s does not match expected %s", id, got[id].Fingerprint, expected[id])
		}
	}
	return nil
}

func readRawMetaPages(path string, pageSize uint32) ([metaPageCount][]byte, error) {
	var pages [metaPageCount][]byte
	f, err := os.Open(path)
	if err != nil {
		return pages, fmt.Errorf("open for meta page reads: %w", err)
	}
	defer f.Close()
	for id := 0; id < metaPageCount; id++ {
		buf := make([]byte, pageSize)
		n, err := f.ReadAt(buf, int64(id)*int64(pageSize))
		if err != nil && err != io.EOF {
			return pages, fmt.Errorf("read meta page %d: %w", id, err)
		}
		if n != len(buf) {
			return pages, fmt.Errorf("short read of meta page %d: got %d bytes, want %d", id, n, len(buf))
		}
		pages[id] = buf
	}
	return pages, nil
}

func metaFingerprint(pageBuf []byte) string {
	region := pageBuf[pageHeaderSize : pageHeaderSize+metaStructSize]
	sum := sha256.Sum256(region)
	return hex.EncodeToString(sum[:])
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if werr != nil {
		return werr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func fingerprintsOf(in [metaPageCount]MetaPageInspection) [metaPageCount]string {
	var out [metaPageCount]string
	for id := 0; id < metaPageCount; id++ {
		out[id] = in[id].Fingerprint
	}
	return out
}

func expectedPostFingerprints(d Decision, pre [metaPageCount]string) [metaPageCount]string {
	if d.Action.Type != ActionCopyPage {
		return pre
	}
	fp := pre[d.Action.From]
	return [metaPageCount]string{fp, fp}
}

func describeAction(a RecoveryAction) string {
	if a.Type == ActionNone {
		return ActionNone
	}
	return fmt.Sprintf("%s %d->%d", a.Type, a.From, a.To)
}

func formatRollback(r *RollbackInfo) string {
	return fmt.Sprintf("rollback: discard meta page %d (txid %d), keep meta page %d (txid %d): %s",
		r.DiscardedPage, r.DiscardedTxid, r.KeptPage, r.KeptTxid, r.Reason)
}

func buildReport(ep *journalEpisode, decision Decision, journalPath string, resumed, alreadyConsistent bool) *RecoveryReport {
	selectedTxid := uint64(0)
	switch {
	case decision.SelectedPage >= 0:
		selectedTxid = ep.Inspections[decision.SelectedPage].Txid
	case decision.Reason == ReasonIdentical:
		// Both pages agree; either txid is the same.
		selectedTxid = ep.Inspections[0].Txid
	}
	return &RecoveryReport{
		SelectedPage:      decision.SelectedPage,
		SelectedTxid:      selectedTxid,
		Reason:            decision.Reason,
		Action:            decision.Action,
		Rollback:          decision.Rollback,
		OutputPath:        ep.OutputPath,
		JournalPath:       journalPath,
		BackupPaths:       append([]string(nil), ep.Backups...),
		Resumed:           resumed,
		AlreadyConsistent: alreadyConsistent,
	}
}

// recordAbortedEpisode best-effort persists the evidence that made recovery
// stop. It never touches the database files.
func recordAbortedEpisode(srcPath, journalPath string, sourceSize int64, pageSize uint32, preFingerprints [metaPageCount]string, inspections [metaPageCount]MetaPageInspection, cause error) {
	journal, _, err := loadJournal(journalPath)
	if err != nil {
		return
	}
	ep := &journalEpisode{
		StartedAt:       nowRFC3339(),
		Status:          statusAborted,
		SourcePath:      srcPath,
		SourceSize:      sourceSize,
		PageSize:        pageSize,
		PreFingerprints: preFingerprints,
		Inspections:     inspections,
		AbortError:      cause.Error(),
	}
	ep.appendEvent(eventPhase, "aborted before any modification")
	ep.appendEvent(eventError, cause.Error())
	journal.beginEpisode(ep)
	_ = saveJournalAtomic(journalPath, journal)
}
