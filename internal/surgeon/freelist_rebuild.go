package surgeon

// Batched, resumable freelist rebuild.
//
// Unlike ClearFreelist (which abandons the freelist and makes bbolt rescan
// the whole database on the next open), this rebuild runs fully offline and
// is driven by an explicit, persisted state file:
//
//   - phase "scan-tree": the b-tree reachable from the active meta page is
//     traversed iteratively in bounded batches, marking every page (including
//     overflow pages) that is actually referenced.
//   - phase "sweep": pages below the high water mark that were not seen
//     during traversal are linearly classified as free, again in batches.
//   - phase "apply": the source file is copied to the destination, a new
//     freelist is appended past the high water mark and a single meta page
//     (the alternating target, exactly like a normal transaction commit) is
//     rewritten in the destination.
//
// The source database is only ever opened read-only, and nothing is written
// (not even the destination file) until every page has been classified and
// zero mismatches were found. If a page is referenced more than once, is
// referenced while still registered in the old freelist, or has any other
// structural inconsistency, the rebuild stops in the terminal "mismatch"
// phase and reports the offending page ids; re-running the command keeps
// returning the same report without touching either file.
//
// Every already-classified page is recorded in bitmaps inside the state
// file, so interrupted runs resume exactly where they stopped and re-running
// a finished rebuild is a no-op.

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/freelist"
)

// Rebuild phases persisted in the checkpoint and reported via
// RebuildProgress.Phase.
const (
	// RebuildPhaseScanTree: reachable pages are still being traversed.
	RebuildPhaseScanTree = "scan-tree"
	// RebuildPhaseSweep: the linear page range is being classified.
	RebuildPhaseSweep = "sweep"
	// RebuildPhaseApply: all pages classified, the output is being written.
	RebuildPhaseApply = "apply"
	// RebuildPhaseDone: the rebuilt database exists and is complete.
	RebuildPhaseDone = "done"
	// RebuildPhaseMismatch: mismatches were found; nothing was written.
	RebuildPhaseMismatch = "mismatch"

	rebuildStateVersion = 1
)

// Mismatch kinds reported through PageMismatch.Kind.
const (
	// MismatchMultipleReferences: a page (or an overflow page in its span)
	// is referenced from more than one place in the b-tree (a cycle has the
	// same shape on disk).
	MismatchMultipleReferences = "multiple-references"
	// MismatchInvalidReference: a tree element points at page id 0/1 or at a
	// page past the high water mark.
	MismatchInvalidReference = "invalid-reference"
	// MismatchOutOfBounds: a page's own overflow span runs past HWM.
	MismatchOutOfBounds = "out-of-bounds"
	// MismatchPageIDMismatch: the id stored in the page header differs from
	// the page's physical offset.
	MismatchPageIDMismatch = "page-id-mismatch"
	// MismatchInvalidPageType: a referenced page is not a branch/leaf page.
	MismatchInvalidPageType = "invalid-page-type"
	// MismatchUnreadablePage: the page could not be read from disk.
	MismatchUnreadablePage = "unreadable-page"
	// MismatchReachableFreed: a reachable page is registered as free in the
	// persisted freelist. Rebuilding would lose live data, so the rebuild
	// refuses to proceed.
	MismatchReachableFreed = "reachable-freed"
	// MismatchInvalidFreelist: the persisted freelist page itself is
	// malformed (bad range, duplicate or out-of-range id, bad type, ...).
	MismatchInvalidFreelist = "invalid-freelist"
)

// ErrRebuildSourceChanged is returned when a resume finds that the source
// database has changed since the state file was created. The state file is
// unusable in that case and must be removed.
var ErrRebuildSourceChanged = errors.New("source database changed since the rebuild state was created; remove the state file and start over")

// ErrRebuildOutputIsSource is returned when the output path is the same as
// the source path; the source file must never be modified in place.
var ErrRebuildOutputIsSource = errors.New("output path must be different from the source database path")

// PageMismatch describes a single page whose on-disk state does not match
// what the freelist rebuild expects.
type PageMismatch struct {
	// PageID is the page the mismatch was detected for.
	PageID common.Pgid `json:"page_id"`
	// Kind is one of the Mismatch* constants.
	Kind string `json:"kind"`
	// Detail is a human readable explanation.
	Detail string `json:"detail"`
	// Stack is the root->page reference path, when known.
	Stack []common.Pgid `json:"stack,omitempty"`
}

func (m PageMismatch) String() string {
	if len(m.Stack) > 0 {
		return fmt.Sprintf("page %d: %s (%s; reference stack: %v)", m.PageID, m.Kind, m.Detail, m.Stack)
	}
	return fmt.Sprintf("page %d: %s (%s)", m.PageID, m.Kind, m.Detail)
}

// RebuildMismatchError wraps all mismatches found during a rebuild. It is
// returned instead of writing anything to the destination file.
type RebuildMismatchError struct {
	Mismatches []PageMismatch
}

func (e *RebuildMismatchError) Error() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "freelist rebuild aborted: %d page mismatch(es) found; the database files were not modified:\n", len(e.Mismatches))
	for _, m := range e.Mismatches {
		fmt.Fprintf(&buf, "  - %s\n", m)
	}
	return buf.String()
}

// RebuildFreelistOptions controls a single (possibly partial) rebuild run.
type RebuildFreelistOptions struct {
	// SourcePath is the read-only source bbolt database.
	SourcePath string
	// OutputPath is where the rebuilt database is written once scanning
	// completes. The file is created during the final "apply" phase and
	// never earlier.
	OutputPath string
	// StatePath is the checkpoint file used to resume interrupted runs.
	// When empty, "<OutputPath>.rebuild-state.json" is used.
	StatePath string
	// BatchPages bounds how many pages a single Run processes (tree pages
	// in the scan phase, linear pages in the sweep phase). <=0 means
	// "process everything remaining in this run".
	BatchPages int
}

// RebuildProgress reports what a single run accomplished.
type RebuildProgress struct {
	// Phase is the phase the rebuild is in after the run.
	Phase string
	// BatchPages is the page budget that was in effect for the run.
	BatchPages int
	// ReachablePages is the number of pages marked reachable so far
	// (overflow pages counted individually).
	ReachablePages int
	// PendingTreePages is the number of discovered but not yet visited
	// tree pages.
	PendingTreePages int
	// SweptPages is the number of pages classified during the sweep so far
	// (counting from page 2).
	SweptPages int
	// TotalPages is the high water mark (meta.pgid) of the source db.
	TotalPages int
	// FreePages is the number of pages currently classified as free.
	FreePages int
	// Done is true once the rebuilt database has been written.
	Done bool
}

// rebuildWorkItem is a pending b-tree page visit plus the reference path
// used for diagnostics.
type rebuildWorkItem struct {
	PageID common.Pgid   `json:"page_id"`
	Stack  []common.Pgid `json:"stack,omitempty"`
}

// freelistRebuildState is the resumable checkpoint persisted as JSON.
type freelistRebuildState struct {
	Version int `json:"version"`

	SourcePath string `json:"source_path"`
	OutputPath string `json:"output_path"`

	// Identity of the source database snapshot this state is valid for.
	PageSize     uint32      `json:"page_size"`
	ActiveMetaID common.Pgid `json:"active_meta_id"`
	MetaTxid     common.Txid `json:"meta_txid"`
	HWMPgid      common.Pgid `json:"hwm_pgid"`
	RootPgid     common.Pgid `json:"root_pgid"`
	FileSize     int64       `json:"file_size"`

	// Phase and accumulated findings.
	Phase         string            `json:"phase"`
	Worklist      []rebuildWorkItem `json:"worklist,omitempty"`
	ReachableBits string            `json:"reachable_bits,omitempty"`
	QueuedBits    string            `json:"queued_bits,omitempty"`
	SweptUntil    common.Pgid       `json:"swept_until"`
	FreePageIDs   []common.Pgid     `json:"free_page_ids,omitempty"`

	// IDs registered in the freelist that was persisted when the rebuild
	// started; empty when the freelist was abandoned.
	OldFreelistIDs []common.Pgid `json:"old_freelist_ids,omitempty"`

	// Mismatches make the rebuild terminal.
	Mismatches []PageMismatch `json:"mismatches,omitempty"`

	// The page the new freelist was written at in the output, for idempotent
	// re-runs.
	NewFreelistPgid common.Pgid `json:"new_freelist_pgid,omitempty"`
}

// RunRebuildFreelist performs one bounded chunk of freelist rebuild work and
// persists progress to the state file. Call it repeatedly with the same
// options until RebuildProgress.Done is true.
//
// The source database is opened read-only. The output database is created
// only during the final run, and only when no mismatches were found.
func RunRebuildFreelist(opts RebuildFreelistOptions) (*RebuildProgress, error) {
	if opts.SourcePath == "" || opts.OutputPath == "" {
		return nil, errors.New("source and output paths are required")
	}
	same, err := samePath(opts.SourcePath, opts.OutputPath)
	if err != nil {
		return nil, err
	}
	if same {
		return nil, ErrRebuildOutputIsSource
	}
	statePath := opts.StatePath
	if statePath == "" {
		statePath = opts.OutputPath + ".rebuild-state.json"
	}

	st, err := loadOrInitRebuildState(statePath, opts)
	if err != nil {
		return nil, err
	}

	progress := &RebuildProgress{Phase: st.Phase, BatchPages: opts.BatchPages, TotalPages: int(st.HWMPgid)}

	// A state created by this or a previous run may already be terminal.
	if st.Phase == RebuildPhaseMismatch {
		if err := persistRebuildState(statePath, st); err != nil {
			return nil, err
		}
		return nil, &RebuildMismatchError{Mismatches: st.Mismatches}
	}

	// A single invocation keeps working across phase boundaries until the
	// page budget is exhausted (BatchPages <= 0 means "run to completion"),
	// so callers that ask for an unlimited batch finish in one invocation.
	pagesProcessed := 0
	for {
		budget := 0
		if opts.BatchPages > 0 {
			budget = opts.BatchPages - pagesProcessed
			if budget <= 0 {
				break
			}
		}
		switch st.Phase {
		case RebuildPhaseScanTree:
			n, err := scanTreeBatch(st, opts.SourcePath, budget)
			if err != nil {
				return nil, err
			}
			pagesProcessed += n
			if len(st.Mismatches) > 0 {
				st.Phase = RebuildPhaseMismatch
				if perr := persistRebuildState(statePath, st); perr != nil {
					return nil, perr
				}
				return nil, &RebuildMismatchError{Mismatches: st.Mismatches}
			}
			if len(st.Worklist) == 0 {
				st.Phase = RebuildPhaseSweep
			}
		case RebuildPhaseSweep:
			n, err := sweepBatch(st, budget)
			if err != nil {
				return nil, err
			}
			pagesProcessed += n
			if st.SweptUntil >= st.HWMPgid {
				st.Phase = RebuildPhaseApply
			}
		case RebuildPhaseApply:
			if err := applyRebuild(st); err != nil {
				return nil, err
			}
			st.Phase = RebuildPhaseDone
		case RebuildPhaseDone:
			// Idempotent re-run: nothing left to do.
		default:
			return nil, fmt.Errorf("unknown rebuild phase %q in state file %s", st.Phase, statePath)
		}
		if st.Phase == RebuildPhaseDone {
			break
		}
		// A bounded batch only visits one page kind per invocation; once
		// the budget is spent, checkpoint and return.
		if opts.BatchPages > 0 && pagesProcessed >= opts.BatchPages {
			break
		}
	}

	if err := persistRebuildState(statePath, st); err != nil {
		return nil, err
	}

	reachable, _ := decodeBitmap(st.ReachableBits)
	progress.Phase = st.Phase
	progress.ReachablePages = popCountBitmap(reachable)
	progress.PendingTreePages = len(st.Worklist)
	progress.SweptPages = int(st.SweptUntil) - 2
	if progress.SweptPages < 0 {
		progress.SweptPages = 0
	}
	progress.FreePages = len(st.FreePageIDs)
	progress.Done = st.Phase == RebuildPhaseDone
	if progress.Done {
		progress.SweptPages = int(st.HWMPgid) - 2
	}
	return progress, nil
}

// --------------------------------------------------------------------------
// State initialization / persistence
// --------------------------------------------------------------------------

func loadOrInitRebuildState(statePath string, opts RebuildFreelistOptions) (*freelistRebuildState, error) {
	data, err := os.ReadFile(statePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read rebuild state %s: %w", statePath, err)
		}
		return initRebuildState(opts)
	}
	var st freelistRebuildState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("parse rebuild state %s: %w", statePath, err)
	}
	if st.Version != rebuildStateVersion {
		return nil, fmt.Errorf("unsupported rebuild state version %d in %s", st.Version, statePath)
	}
	if st.SourcePath != opts.SourcePath || st.OutputPath != opts.OutputPath {
		return nil, fmt.Errorf("rebuild state %s was created for source=%q output=%q, got source=%q output=%q",
			statePath, st.SourcePath, st.OutputPath, opts.SourcePath, opts.OutputPath)
	}
	// The source database must be byte-identical at the meta level for a
	// resume to be meaningful.
	if err := checkSourceIdentity(&st); err != nil {
		return nil, err
	}
	return &st, nil
}

func initRebuildState(opts RebuildFreelistOptions) (*freelistRebuildState, error) {
	f, err := os.Open(opts.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("open source database: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	pageSize, active, err := readActiveMeta(f)
	if err != nil {
		return nil, err
	}

	st := &freelistRebuildState{
		Version:    rebuildStateVersion,
		SourcePath: opts.SourcePath,
		OutputPath: opts.OutputPath,

		PageSize:     pageSize,
		ActiveMetaID: active.id,
		MetaTxid:     active.meta.Txid(),
		HWMPgid:      active.meta.Pgid(),
		RootPgid:     active.meta.RootBucket().RootPage(),
		FileSize:     fi.Size(),

		Phase:       RebuildPhaseScanTree,
		SweptUntil:  2, // pages 0 and 1 are reserved for meta pages.
		FreePageIDs: []common.Pgid{},
	}

	// Record the ids registered in any persisted freelist. They are only
	// needed to detect reachable-but-freed conflicts; the rebuilt freelist
	// itself is derived purely from page reachability.
	if active.meta.Freelist() != common.PgidNoFreelist {
		readAndValidateOldFreelist(f, pageSize, st, active.meta.Freelist())
	}

	if len(st.Mismatches) == 0 && st.RootPgid >= 2 && st.RootPgid < st.HWMPgid {
		st.Worklist = []rebuildWorkItem{{PageID: st.RootPgid, Stack: []common.Pgid{st.RootPgid}}}
		st.QueuedBits = encodeBitmap(setBit(nil, uint64(st.RootPgid)))
	}
	if len(st.Mismatches) > 0 {
		st.Phase = RebuildPhaseMismatch
	}
	st.ReachableBits = encodeBitmap(nil)
	return st, nil
}

func checkSourceIdentity(st *freelistRebuildState) error {
	f, err := os.Open(st.SourcePath)
	if err != nil {
		return fmt.Errorf("open source database: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	pageSize, active, err := readActiveMeta(f)
	if err != nil {
		return ErrRebuildSourceChanged
	}
	if fi.Size() != st.FileSize ||
		pageSize != st.PageSize ||
		active.id != st.ActiveMetaID ||
		active.meta.Txid() != st.MetaTxid ||
		active.meta.Pgid() != st.HWMPgid ||
		active.meta.RootBucket().RootPage() != st.RootPgid {
		return ErrRebuildSourceChanged
	}
	return nil
}

func persistRebuildState(path string, st *freelistRebuildState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write rebuild state %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("publish rebuild state %s: %w", path, err)
	}
	return nil
}

// activeMeta bundles the chosen meta page with its physical page id.
type activeMeta struct {
	meta *common.Meta
	id   common.Pgid
}

// readActiveMeta reads both meta pages and returns the one with the highest
// transaction id, requiring its checksum to be valid.
func readActiveMeta(f *os.File) (uint32, *activeMeta, error) {
	// Read meta page 0 first only to learn the page size.
	head := make([]byte, 4096)
	if _, err := f.ReadAt(head, 0); err != nil && !errors.Is(err, io.EOF) {
		return 0, nil, fmt.Errorf("read meta page 0: %w", err)
	}
	m0 := common.LoadPageMeta(head)
	pageSize := int(m0.PageSize())
	if pageSize <= 0 {
		return 0, nil, fmt.Errorf("invalid page size %d in meta page 0", pageSize)
	}

	buf0 := make([]byte, pageSize)
	if _, err := f.ReadAt(buf0, 0); err != nil && !errors.Is(err, io.EOF) {
		return 0, nil, fmt.Errorf("read meta page 0: %w", err)
	}
	buf1 := make([]byte, pageSize)
	if _, err := f.ReadAt(buf1, int64(pageSize)); err != nil && !errors.Is(err, io.EOF) {
		return 0, nil, fmt.Errorf("read meta page 1: %w", err)
	}
	meta0 := common.LoadPageMeta(buf0)
	meta1 := common.LoadPageMeta(buf1)

	var active *activeMeta
	if meta0.Txid() >= meta1.Txid() {
		active = &activeMeta{meta: meta0, id: 0}
	} else {
		active = &activeMeta{meta: meta1, id: 1}
	}
	if err := active.meta.Validate(); err != nil {
		return 0, nil, fmt.Errorf("active meta page %d is invalid: %w", active.id, err)
	}
	return uint32(pageSize), active, nil
}

// readAndValidateOldFreelist reads the persisted freelist and makes sure it
// is structurally sane. Every problem is recorded as a mismatch on st.
func readAndValidateOldFreelist(f *os.File, pageSize uint32, st *freelistRebuildState, flID common.Pgid) {
	add := func(id common.Pgid, kind, detail string) {
		st.Mismatches = append(st.Mismatches, PageMismatch{PageID: id, Kind: kind, Detail: detail})
	}

	if flID < 2 || flID >= st.HWMPgid {
		add(flID, MismatchInvalidFreelist, fmt.Sprintf("freelist pointer %d is out of range [2, %d)", flID, st.HWMPgid))
		return
	}

	header := make([]byte, pageSize)
	if _, err := f.ReadAt(header, int64(flID)*int64(pageSize)); err != nil {
		add(flID, MismatchUnreadablePage, fmt.Sprintf("cannot read persisted freelist page: %v", err))
		return
	}
	hp := common.LoadPage(header)
	if hp.Id() != flID {
		add(flID, MismatchPageIDMismatch, fmt.Sprintf("persisted freelist page identifies as page %d", hp.Id()))
		return
	}
	if !hp.IsFreelistPage() {
		add(flID, MismatchInvalidFreelist, fmt.Sprintf("meta freelist pointer %d targets a %s page", flID, hp.Typ()))
		return
	}
	spanEnd := uint64(flID) + uint64(hp.Overflow()) + 1
	if spanEnd > uint64(st.HWMPgid) {
		add(flID, MismatchOutOfBounds, fmt.Sprintf("persisted freelist overflow span [%d,%d) runs past high water mark %d", flID, spanEnd, st.HWMPgid))
		return
	}

	buf := make([]byte, (uint64(hp.Overflow())+1)*uint64(pageSize))
	if _, err := f.ReadAt(buf, int64(flID)*int64(pageSize)); err != nil {
		add(flID, MismatchUnreadablePage, fmt.Sprintf("cannot read persisted freelist page span: %v", err))
		return
	}
	rawIDs := common.LoadPage(buf).FreelistPageIds()
	ids := make([]common.Pgid, len(rawIDs))
	copy(ids, rawIDs)

	seen := make(map[common.Pgid]struct{}, len(ids))
	var prev common.Pgid
	for i, id := range ids {
		if id < 2 || id >= st.HWMPgid {
			add(id, MismatchInvalidFreelist, fmt.Sprintf("freelist entry #%d is out of range [2, %d)", i, st.HWMPgid))
		} else if i > 0 && id <= prev {
			if _, dup := seen[id]; dup {
				add(id, MismatchInvalidFreelist, "freelist entry is registered twice")
			} else {
				add(id, MismatchInvalidFreelist, fmt.Sprintf("freelist entries are not strictly sorted (entry #%d: %d after %d)", i, id, prev))
			}
		}
		seen[id] = struct{}{}
		prev = id
	}
	if len(st.Mismatches) == 0 {
		st.OldFreelistIDs = ids
	}
}

// --------------------------------------------------------------------------
// Phase 1: iterative, batched reachability traversal
// --------------------------------------------------------------------------

func scanTreeBatch(st *freelistRebuildState, sourcePath string, budget int) (int, error) {
	f, err := os.Open(sourcePath)
	if err != nil {
		return 0, fmt.Errorf("open source database: %w", err)
	}
	defer f.Close()

	reachableRaw, err := decodeBitmap(st.ReachableBits)
	if err != nil {
		return 0, err
	}
	queuedRaw, err := decodeBitmap(st.QueuedBits)
	if err != nil {
		return 0, err
	}
	oldFree := st.OldFreelistIDs

	processed := 0
	for len(st.Worklist) > 0 && (budget <= 0 || processed < budget) {
		// Pop the most recently queued page (depth-first): the persisted
		// frontier stays small.
		last := len(st.Worklist) - 1
		item := st.Worklist[last]
		st.Worklist = st.Worklist[:last]
		processed++

		children, reachableUpdated, stop := visitTreePage(f, st.PageSize, st.HWMPgid, item, reachableRaw, oldFree, &st.Mismatches)
		reachableRaw = reachableUpdated
		if !stop {
			// Enqueue children that are not already visited or queued.
			for _, child := range children {
				if hasBit(reachableRaw, uint64(child)) || hasBit(queuedRaw, uint64(child)) {
					st.Mismatches = append(st.Mismatches, PageMismatch{
						PageID: child,
						Kind:   MismatchMultipleReferences,
						Detail: "page is referenced from more than one b-tree location",
						Stack:  append(copyPgids(item.Stack), child),
					})
					continue
				}
				queuedRaw = setBit(queuedRaw, uint64(child))
				st.Worklist = append(st.Worklist, rebuildWorkItem{
					PageID: child,
					Stack:  append(copyPgids(item.Stack), child),
				})
			}
		}
	}

	st.ReachableBits = encodeBitmap(reachableRaw)
	st.QueuedBits = encodeBitmap(queuedRaw)
	return processed, nil
}

// visitTreePage validates one reachable page, marks its whole span reachable
// and returns the page ids its references. stop is true when the page is so
// badly malformed that descending into it would only produce noise.
func visitTreePage(
	f *os.File, pageSize uint32, hwm common.Pgid,
	item rebuildWorkItem,
	reachable []byte, oldFree []common.Pgid,
	mismatches *[]PageMismatch,
) (children []common.Pgid, reachableOut []byte, stop bool) {
	id := item.PageID

	if id < 2 || id >= hwm {
		*mismatches = append(*mismatches, PageMismatch{
			PageID: id,
			Kind:   MismatchInvalidReference,
			Detail: fmt.Sprintf("tree references a page outside [2, %d)", hwm),
			Stack:  item.Stack,
		})
		return nil, reachable, true
	}

	header := make([]byte, pageSize)
	if _, err := f.ReadAt(header, int64(id)*int64(pageSize)); err != nil {
		*mismatches = append(*mismatches, PageMismatch{
			PageID: id,
			Kind:   MismatchUnreadablePage,
			Detail: err.Error(),
			Stack:  item.Stack,
		})
		return nil, reachable, true
	}
	hp := common.LoadPage(header)
	if hp.Id() != id {
		*mismatches = append(*mismatches, PageMismatch{
			PageID: id,
			Kind:   MismatchPageIDMismatch,
			Detail: fmt.Sprintf("page header identifies this offset as page %d", hp.Id()),
			Stack:  item.Stack,
		})
		return nil, reachable, true
	}

	spanEnd := uint64(id) + uint64(hp.Overflow()) + 1
	if spanEnd > uint64(hwm) {
		*mismatches = append(*mismatches, PageMismatch{
			PageID: id,
			Kind:   MismatchOutOfBounds,
			Detail: fmt.Sprintf("page overflow span [%d,%d) runs past high water mark %d", id, spanEnd, hwm),
			Stack:  item.Stack,
		})
		return nil, reachable, true
	}

	if !hp.IsBranchPage() && !hp.IsLeafPage() {
		*mismatches = append(*mismatches, PageMismatch{
			PageID: id,
			Kind:   MismatchInvalidPageType,
			Detail: fmt.Sprintf("referenced page has flags %04x (%s)", hp.Flags(), hp.Typ()),
			Stack:  item.Stack,
		})
		return nil, reachable, true
	}

	// The full span is needed because leaf element values (bucket headers)
	// can spill into overflow pages.
	buf := header
	if hp.Overflow() > 0 {
		buf = make([]byte, spanEnd-uint64(id))
		if _, err := f.ReadAt(buf, int64(id)*int64(pageSize)); err != nil {
			*mismatches = append(*mismatches, PageMismatch{
				PageID: id,
				Kind:   MismatchUnreadablePage,
				Detail: err.Error(),
				Stack:  item.Stack,
			})
			return nil, reachable, true
		}
		hp = common.LoadPage(buf)
	}
	p := hp

	// Every page of the span must be claimed exactly once, and none of them
	// may be registered in the old freelist.
	for q := uint64(id); q < spanEnd; q++ {
		if hasBit(reachable, q) {
			*mismatches = append(*mismatches, PageMismatch{
				PageID: common.Pgid(q),
				Kind:   MismatchMultipleReferences,
				Detail: fmt.Sprintf("overflow span starting at page %d overlaps an already referenced page", id),
				Stack:  item.Stack,
			})
		}
		idx := sort.Search(len(oldFree), func(i int) bool { return oldFree[i] >= common.Pgid(q) })
		if idx < len(oldFree) && oldFree[idx] == common.Pgid(q) {
			*mismatches = append(*mismatches, PageMismatch{
				PageID: common.Pgid(q),
				Kind:   MismatchReachableFreed,
				Detail: "page is referenced by the b-tree but registered as free in the persisted freelist",
				Stack:  item.Stack,
			})
		}
	}
	for q := uint64(id); q < spanEnd; q++ {
		reachable = setBit(reachable, q)
	}

	switch {
	case p.IsBranchPage():
		for i := range p.BranchPageElements() {
			children = append(children, p.BranchPageElement(uint16(i)).Pgid())
		}
	case p.IsLeafPage():
		for i := range p.LeafPageElements() {
			elem := p.LeafPageElement(uint16(i))
			if !elem.IsBucketEntry() {
				continue
			}
			// Inline buckets (root == 0) live entirely inside the leaf
			// value and reference no on-disk pages.
			if root := elem.Bucket().RootPage(); root > 0 {
				children = append(children, root)
			}
		}
	}
	return children, reachable, false
}

// --------------------------------------------------------------------------
// Phase 2: batched linear sweep classifying unreferenced pages as free
// --------------------------------------------------------------------------

func sweepBatch(st *freelistRebuildState, budget int) (int, error) {
	reachableRaw, err := decodeBitmap(st.ReachableBits)
	if err != nil {
		// A corrupt state file must not drive a rebuild with partial data.
		return 0, err
	}

	cursor := st.SweptUntil
	processed := 0
	for cursor < st.HWMPgid && (budget <= 0 || processed < budget) {
		if !hasBit(reachableRaw, uint64(cursor)) {
			st.FreePageIDs = append(st.FreePageIDs, cursor)
		}
		cursor++
		processed++
	}
	st.SweptUntil = cursor
	return processed, nil
}

// --------------------------------------------------------------------------
// Phase 3: copy source -> output and write the rebuilt freelist offline
// --------------------------------------------------------------------------

func applyRebuild(st *freelistRebuildState) error {
	// Re-validate the source identity right before touching the output.
	if err := checkSourceIdentity(st); err != nil {
		return err
	}

	// A previous attempt may have crashed mid-apply and left a partial
	// output file behind; the output is ours, so remove it and start the
	// apply over from the byte-identical source copy.
	if _, err := os.Stat(st.OutputPath); err == nil {
		if err := os.Remove(st.OutputPath); err != nil {
			return fmt.Errorf("remove stale output %s: %w", st.OutputPath, err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := common.CopyFile(st.SourcePath, st.OutputPath); err != nil {
		return fmt.Errorf("copy source to output: %w", err)
	}

	f, err := os.OpenFile(st.OutputPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open output for freelist write: %w", err)
	}
	defer f.Close()

	pageSize := int(st.PageSize)
	hwm := st.HWMPgid

	// Serialize the free ids exactly the way bbolt would.
	fl := freelist.NewArrayFreelist()
	fl.Init(st.FreePageIDs)
	nPages := fl.EstimatedWritePageSize()/pageSize + 1
	flBuf := make([]byte, nPages*pageSize)
	flPage := common.LoadPage(flBuf)
	flPage.SetId(hwm)
	fl.Write(flPage)
	flPage.SetOverflow(uint32(nPages - 1))

	if _, err := f.WriteAt(flBuf, int64(hwm)*int64(pageSize)); err != nil {
		return fmt.Errorf("write rebuilt freelist pages: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync rebuilt freelist pages: %w", err)
	}

	// Commit the new freelist like a single bbolt transaction would:
	// bump txid (alternating the target meta page), move HWM past the new
	// freelist pages, recompute the checksum.
	activeBuf := make([]byte, pageSize)
	if _, err := f.ReadAt(activeBuf, int64(st.ActiveMetaID)*int64(pageSize)); err != nil {
		return fmt.Errorf("read active meta page: %w", err)
	}
	meta := common.LoadPageMeta(activeBuf)
	meta.SetFreelist(hwm)
	meta.SetPgid(hwm + common.Pgid(nPages))
	meta.IncTxid()
	targetID := common.Pgid(meta.Txid() % 2)

	targetBuf := make([]byte, pageSize)
	if _, err := f.ReadAt(targetBuf, int64(targetID)*int64(pageSize)); err != nil {
		return fmt.Errorf("read target meta page: %w", err)
	}
	meta.Write(common.LoadPage(targetBuf))
	if _, err := f.WriteAt(targetBuf, int64(targetID)*int64(pageSize)); err != nil {
		return fmt.Errorf("write target meta page: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync target meta page: %w", err)
	}

	st.NewFreelistPgid = hwm
	return nil
}

// --------------------------------------------------------------------------
// Small helpers
// --------------------------------------------------------------------------

func copyPgids(in []common.Pgid) []common.Pgid {
	if len(in) == 0 {
		return nil
	}
	out := make([]common.Pgid, len(in))
	copy(out, in)
	return out
}

func samePath(a, b string) (bool, error) {
	aa, err := filepath.Abs(a)
	if err != nil {
		return false, err
	}
	bb, err := filepath.Abs(b)
	if err != nil {
		return false, err
	}
	return aa == bb, nil
}

// --- dense bitmap (bit i <=> page i), persisted gzip+base64 ---------------

func setBit(bits []byte, i uint64) []byte {
	need := int(i/8) + 1
	if len(bits) < need {
		grown := make([]byte, need)
		copy(grown, bits)
		bits = grown
	}
	bits[i/8] |= 1 << (i % 8)
	return bits
}

func hasBit(bits []byte, i uint64) bool {
	if int(i/8) >= len(bits) {
		return false
	}
	return bits[i/8]&(1<<(i%8)) != 0
}

func popCountBitmap(bits []byte) int {
	n := 0
	for _, b := range bits {
		for b != 0 {
			b &= b - 1
			n++
		}
	}
	return n
}

func encodeBitmap(bits []byte) string {
	if len(bits) == 0 {
		return ""
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(bits)
	_ = gz.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func decodeBitmap(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode rebuild bitmap: %w", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decompress rebuild bitmap: %w", err)
	}
	defer gz.Close()
	out, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("decompress rebuild bitmap: %w", err)
	}
	return out, nil
}
