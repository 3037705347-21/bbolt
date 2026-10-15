package surgeon

// Offline recovery support for diverged/corrupted bbolt meta pages.
//
// After a power loss the two meta pages may disagree or one of them may be
// torn. The entry point is RecoverMetaPages, which inspects both pages
// read-only, picks a version deterministically, repairs a copy of the
// database and records the whole decision trail in a journal so that an
// interrupted run resumes to exactly the same conclusion.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// Sentinel errors reported by the meta recovery flow. Callers should use
// errors.Is to detect them.
var (
	// ErrBothMetaPagesInvalid is returned when neither meta page passes
	// validation. The database file is never modified in that case.
	ErrBothMetaPagesInvalid = errors.New("both meta pages are invalid")
	// ErrMetaPagesAmbiguous is returned when both meta pages validate,
	// carry the same txid but disagree on their contents. An explicit
	// page selection is required to proceed.
	ErrMetaPagesAmbiguous = errors.New("meta pages are ambiguous")
	// ErrRecoveryStateMismatch is returned when the current output file
	// matches neither the recorded pre-recovery nor post-recovery state.
	// Recovery refuses to guess and never performs a different action
	// than the one recorded in the journal.
	ErrRecoveryStateMismatch = errors.New("recovery state mismatch")
)

// Decision reason codes. They are persisted into the recovery journal, so
// treat the values as a stable API.
const (
	ReasonOnlyValid      = "only-valid"
	ReasonHigherTxid     = "higher-txid"
	ReasonIdentical      = "identical"
	ReasonOperatorForced = "operator-forced"
)

// Recovery action types.
const (
	ActionNone     = "none"
	ActionCopyPage = "copy-page"
)

const (
	// metaPageCount is the fixed number of meta pages in a bbolt file.
	metaPageCount = 2
	// probeChunkSize is enough to read the 16B page header and the 64B meta.
	probeChunkSize = 1024
	// largestPageSizeShift mirrors the upper bound used by db.getPageSize.
	largestPageSizeShift = 14
)

var (
	pageHeaderSize = int(common.PageHeaderSize)
	metaStructSize = int(unsafe.Sizeof(common.Meta{}))
)

// MetaPageInspection is the full evidence collected for a single meta page.
// It is persisted (as JSON) into the recovery journal; the raw page bytes
// themselves are kept only in memory and therefore unexported.
type MetaPageInspection struct {
	// PageID is the physical page position (0 or 1).
	PageID uint64 `json:"pageId"`
	// Readable reports whether the page bytes could be read from disk.
	Readable bool `json:"readable"`
	// ReadError carries the read failure when Readable is false.
	ReadError string `json:"readError,omitempty"`

	// PageSize is the page size used when locating/reading this page.
	PageSize uint32 `json:"pageSize"`

	// Page header evidence.
	HeaderFlags   uint16 `json:"headerFlags"`
	HeaderID      uint64 `json:"headerId"`
	HeaderFlagsOK bool   `json:"headerFlagsOk"`
	HeaderIDOK    bool   `json:"headerIdOk"`

	// Validation evidence.
	MagicOK          bool   `json:"magicOk"`
	VersionOK        bool   `json:"versionOk"`
	ChecksumOK       bool   `json:"checksumOk"`
	ValidateError    string `json:"validateError,omitempty"`
	ExpectedChecksum uint64 `json:"expectedChecksum"`

	// Meta fields (readable even when the checksum is wrong).
	Txid         uint64 `json:"txid"`
	Checksum     uint64 `json:"checksum"`
	Root         uint64 `json:"root"`
	RootSequence uint64 `json:"rootSequence"`
	Freelist     uint64 `json:"freelist"`
	Pgid         uint64 `json:"pgid"`
	MetaPageSize uint32 `json:"metaPageSize"`

	// Derived evidence.
	ParityOK     bool   `json:"parityOk"`
	StructuralOK bool   `json:"structuralOk"`
	Fingerprint  string `json:"fingerprint"`

	// raw contains the full page bytes; retained in memory for the
	// backup/copy steps and never serialized into the journal.
	raw []byte
}

// RawPage returns the raw bytes of the inspected page; nil when unreadable.
func (m *MetaPageInspection) RawPage() []byte {
	return m.raw
}

// Valid reports whether the page passes common.Meta.Validate().
func (m *MetaPageInspection) Valid() bool {
	return m.Readable && m.ValidateError == ""
}

// RecoveryAction describes what is done to the output file.
type RecoveryAction struct {
	Type string `json:"type"`
	// From and To are physical meta page ids for ActionCopyPage.
	From uint64 `json:"from,omitempty"`
	To   uint64 `json:"to,omitempty"`
}

// RollbackInfo records a discarded meta version for auditability.
type RollbackInfo struct {
	// DiscardedPage is the page that was not selected (-1 when none).
	DiscardedPage int `json:"discardedPage"`
	// DiscardedTxid is the txid claimed by the discarded page. It may be
	// 0 when the discarded page was too corrupted to expose a txid.
	DiscardedTxid uint64 `json:"discardedTxid"`
	// KeptPage is the selected page.
	KeptPage int `json:"keptPage"`
	// KeptTxid is the txid of the selected page.
	KeptTxid uint64 `json:"keptTxid"`
	// Reason explains the rollback (torn newer write, operator forced ...).
	Reason string `json:"reason"`
}

// Decision is the deterministic output of the meta page selection policy.
type Decision struct {
	// SelectedPage is the chosen physical page id, or -1 when the pages
	// are identical and no action is needed.
	SelectedPage int            `json:"selectedPage"`
	Reason       string         `json:"reason"`
	Action       RecoveryAction `json:"action"`
	// Rollback is non-nil when a version is discarded.
	Rollback *RollbackInfo `json:"rollback,omitempty"`
	// DivergingFields lists the meta fields that disagree when the pages
	// carry the same txid but different contents.
	DivergingFields []string `json:"divergingFields,omitempty"`
}

// inspectMetaPages reads both meta pages without any mmap and without
// assuming the page size is known ahead of time.
func inspectMetaPages(path string) (uint32, [metaPageCount]MetaPageInspection, error) {
	var inspections [metaPageCount]MetaPageInspection

	f, err := os.Open(path)
	if err != nil {
		return 0, inspections, fmt.Errorf("open database for inspection: %w", err)
	}
	defer f.Close()

	pageSize := detectMetaPageSize(f)

	fileSize := int64(-1)
	if fi, statErr := f.Stat(); statErr == nil {
		fileSize = fi.Size()
	}

	for id := 0; id < metaPageCount; id++ {
		inspections[id] = inspectMetaPage(f, uint64(id), pageSize, fileSize)
	}
	return pageSize, inspections, nil
}

// InspectMetaPages exposes the read-only meta page diagnosis. It performs
// no writes of any kind.
func InspectMetaPages(path string) (uint32, [metaPageCount]MetaPageInspection, error) {
	return inspectMetaPages(path)
}

// detectMetaPageSize determines the page size from the raw meta pages. It
// mirrors db.getPageSize: trust meta0 when its magic is right (its fields
// are readable even when the checksum fails), otherwise probe meta1 at
// candidate offsets. Returns 0 when neither page exposes a valid magic.
func detectMetaPageSize(f *os.File) uint32 {
	var buf [probeChunkSize]byte
	if n, err := f.ReadAt(buf[:], 0); err != nil && err != io.EOF {
		return 0
	} else if n >= pageHeaderSize+metaStructSize {
		m := common.LoadPageMeta(buf[:])
		if m.Magic() == common.Magic && isPlausiblePageSize(m.PageSize()) {
			return m.PageSize()
		}
	}

	fi, err := f.Stat()
	if err != nil {
		return 0
	}
	fileSize := fi.Size()
	for i := 0; i <= largestPageSizeShift; i++ {
		pos := int64(1024 << uint(i))
		if pos >= fileSize-probeChunkSize {
			break
		}
		n, readErr := f.ReadAt(buf[:], pos)
		if readErr != nil && readErr != io.EOF {
			continue
		}
		if n < pageHeaderSize+metaStructSize {
			continue
		}
		m := common.LoadPageMeta(buf[:])
		if m.Magic() == common.Magic && isPlausiblePageSize(m.PageSize()) {
			return m.PageSize()
		}
	}
	return 0
}

func isPlausiblePageSize(size uint32) bool {
	if size < 1024 {
		return false
	}
	// Page sizes are powers of two.
	return size&(size-1) == 0
}

func inspectMetaPage(f *os.File, pageID uint64, pageSize uint32, fileSize int64) MetaPageInspection {
	insp := MetaPageInspection{PageID: pageID}

	if pageSize == 0 {
		insp.ReadError = "page size unknown: neither meta page exposes a valid magic"
		return insp
	}
	insp.PageSize = pageSize

	offset := int64(pageID) * int64(pageSize)
	if fileSize >= 0 && fileSize < offset+int64(pageSize) {
		insp.ReadError = fmt.Sprintf("file too small: meta page %d (%d bytes at offset %d) exceeds file size %d", pageID, pageSize, offset, fileSize)
		return insp
	}

	buf := make([]byte, pageSize)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		insp.ReadError = err.Error()
		return insp
	}
	if n != len(buf) {
		insp.ReadError = fmt.Sprintf("short read: got %d bytes, want %d", n, len(buf))
		return insp
	}
	insp.Readable = true
	insp.raw = buf

	p := common.LoadPage(buf)
	m := common.LoadPageMeta(buf)

	insp.HeaderFlags = p.Flags()
	insp.HeaderID = uint64(p.Id())
	insp.HeaderFlagsOK = p.IsMetaPage()
	insp.HeaderIDOK = p.Id() == common.Pgid(pageID)

	insp.MagicOK = m.Magic() == common.Magic
	insp.VersionOK = m.Version() == common.Version
	insp.Checksum = m.Checksum()
	insp.ExpectedChecksum = m.Sum64()
	insp.ChecksumOK = m.Checksum() == m.Sum64()

	insp.Txid = uint64(m.Txid())
	insp.Root = uint64(m.RootBucket().RootPage())
	insp.RootSequence = m.RootBucket().InSequence()
	insp.Freelist = uint64(m.Freelist())
	insp.Pgid = uint64(m.Pgid())
	insp.MetaPageSize = m.PageSize()

	insp.ParityOK = m.Txid()%2 == common.Txid(pageID)
	insp.StructuralOK = structuralSound(m)

	metaRegion := buf[pageHeaderSize : pageHeaderSize+metaStructSize]
	sum := sha256.Sum256(metaRegion)
	insp.Fingerprint = hex.EncodeToString(sum[:])

	insp.ValidateError = classifyValidateError(m)
	return insp
}

func structuralSound(m *common.Meta) bool {
	hwm := m.Pgid()
	if m.RootBucket().RootPage() >= hwm {
		return false
	}
	fl := m.Freelist()
	if fl != common.PgidNoFreelist && fl >= hwm {
		return false
	}
	return true
}

func classifyValidateError(m *common.Meta) string {
	if err := m.Validate(); err != nil {
		return err.Error()
	}
	return ""
}

// DecideMetaRecovery applies the deterministic selection policy to the two
// page inspections. It is a pure function: the same input always yields the
// same decision.
//
// forcedPage must be nil unless the pages are ambiguous (both valid, same
// txid, different contents), in which case it must point to 0 or 1. Passing
// it in any other situation is a caller error.
func DecideMetaRecovery(inspections [metaPageCount]MetaPageInspection, forcedPage *int) (Decision, error) {
	if forcedPage != nil && (*forcedPage < 0 || *forcedPage >= metaPageCount) {
		return Decision{}, fmt.Errorf("invalid forced meta page %d: must be 0 or 1", *forcedPage)
	}

	m0, m1 := inspections[0], inspections[1]
	v0, v1 := m0.Valid(), m1.Valid()

	switch {
	case !v0 && !v1:
		return Decision{}, fmt.Errorf("%w: page 0: %s; page 1: %s",
			ErrBothMetaPagesInvalid, pageFailure(m0), pageFailure(m1))
	case v0 && !v1:
		return selectSoleValidPage(0, m0, m1), nil
	case v1 && !v0:
		return selectSoleValidPage(1, m1, m0), nil
	}

	// Both pages validate.
	if m0.Txid != m1.Txid {
		if m0.Txid > m1.Txid {
			return selectHigherTxid(0, m0, 1), nil
		}
		return selectHigherTxid(1, m1, 0), nil
	}

	// Same txid.
	if m0.Fingerprint == m1.Fingerprint {
		return Decision{
			SelectedPage: -1,
			Reason:       ReasonIdentical,
			Action:       RecoveryAction{Type: ActionNone},
		}, nil
	}

	diverging := divergingFields(m0, m1)
	if forcedPage == nil {
		return Decision{}, fmt.Errorf("%w: both meta pages are valid with txid %d but contents differ (%s); re-run with an explicit --select-meta-page 0|1 to choose",
			ErrMetaPagesAmbiguous, m0.Txid, joinFields(diverging))
	}

	selected := *forcedPage
	other := 1 - selected
	d := Decision{
		SelectedPage: selected,
		Reason:       ReasonOperatorForced,
		Action: RecoveryAction{
			Type: ActionCopyPage,
			From: uint64(selected),
			To:   uint64(other),
		},
		DivergingFields: diverging,
		Rollback: &RollbackInfo{
			DiscardedPage: other,
			DiscardedTxid: inspections[other].Txid,
			KeptPage:      selected,
			KeptTxid:      inspections[selected].Txid,
			Reason:        "operator forced resolution of ambiguous same-txid divergence",
		},
	}
	return d, nil
}

// IsAmbiguous reports whether err is an ambiguity decision error.
func IsAmbiguous(err error) bool {
	return errors.Is(err, ErrMetaPagesAmbiguous)
}

func pageFailure(m MetaPageInspection) string {
	if m.ReadError != "" {
		return fmt.Sprintf("unreadable: %s", m.ReadError)
	}
	if m.ValidateError != "" {
		return m.ValidateError
	}
	return berrors.ErrInvalid.Error()
}

func selectSoleValidPage(selected int, kept, damaged MetaPageInspection) Decision {
	other := 1 - selected
	d := Decision{
		SelectedPage: selected,
		Reason:       ReasonOnlyValid,
		Action: RecoveryAction{
			Type: ActionCopyPage,
			From: uint64(selected),
			To:   uint64(other),
		},
	}
	// If the torn page claims a higher txid, selecting the valid page
	// rolls back one transaction; record it.
	if damaged.MagicOK && damaged.Txid > kept.Txid {
		d.Rollback = &RollbackInfo{
			DiscardedPage: other,
			DiscardedTxid: damaged.Txid,
			KeptPage:      selected,
			KeptTxid:      kept.Txid,
			Reason:        "discarded torn meta page claiming a higher txid (checksum invalid)",
		}
	}
	return d
}

func selectHigherTxid(selected int, kept MetaPageInspection, other int) Decision {
	return Decision{
		SelectedPage: selected,
		Reason:       ReasonHigherTxid,
		Action: RecoveryAction{
			Type: ActionCopyPage,
			From: uint64(selected),
			To:   uint64(other),
		},
	}
}

// divergingFields returns the meta fields that differ between two
// same-txid meta pages, in a fixed order for deterministic output.
func divergingFields(a, b MetaPageInspection) []string {
	var fields []string
	if a.Root != b.Root {
		fields = append(fields, fmt.Sprintf("root: %d != %d", a.Root, b.Root))
	}
	if a.RootSequence != b.RootSequence {
		fields = append(fields, fmt.Sprintf("rootSequence: %d != %d", a.RootSequence, b.RootSequence))
	}
	if a.Freelist != b.Freelist {
		fields = append(fields, fmt.Sprintf("freelist: %d != %d", a.Freelist, b.Freelist))
	}
	if a.Pgid != b.Pgid {
		fields = append(fields, fmt.Sprintf("pgid: %d != %d", a.Pgid, b.Pgid))
	}
	if a.MetaPageSize != b.MetaPageSize {
		fields = append(fields, fmt.Sprintf("pageSize: %d != %d", a.MetaPageSize, b.MetaPageSize))
	}
	if a.Checksum != b.Checksum {
		fields = append(fields, "checksum differs")
	}
	if a.HeaderFlags != b.HeaderFlags {
		fields = append(fields, fmt.Sprintf("headerFlags: %#x != %#x", a.HeaderFlags, b.HeaderFlags))
	}
	return fields
}

func joinFields(fields []string) string {
	out := ""
	for i, f := range fields {
		if i > 0 {
			out += "; "
		}
		out += f
	}
	return out
}
