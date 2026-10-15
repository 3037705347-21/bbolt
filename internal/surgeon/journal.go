package surgeon

// Crash safe page repair journal.
//
// A repair journal is an append-only write-ahead log stored next to the
// database file being repaired (see JournalSuffix). Every page mutation
// records:
//   - what is being done (operation name) and the target page id;
//   - the full before-image of the page (its original bytes, including
//     any overflow span);
//   - the full after-image of the page (the bytes that must be on disk
//     once the repair finished);
//   - a COMMIT marker which is appended and fsynced only after the page
//     itself was written, fsynced and read back from disk.
//
// Crash recovery rules (executed by ResumeRepair/Recover):
//   - COMMIT present and disk page matches the after-image: nothing to
//     do, the page is confirmed to belong to this repair version;
//   - COMMIT present but disk page differs (torn/lost write): the
//     after-image is written again (redo);
//   - no COMMIT marker (the repair died mid-write): the before-image is
//     restored (rollback) and an ABORT marker is durably recorded, so no
//     half-modified page is left behind and the abort itself survives
//     the next crash.
//
// The journal header binds the repair to one concrete source database
// version: page size, file size, active txid and SHA-256 hashes of both
// meta pages. Verify re-checks this identity and every finished page, so
// a repaired file can never be confused with a different version.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

// JournalSuffix is appended to the repaired database path to get the
// journal file path.
const JournalSuffix = ".surgery-journal"

const (
	journalMagic   = "BBJRNL01"
	journalVersion = uint16(1)

	recHeader   uint16 = 1
	recApply    uint16 = 2
	recCommit   uint16 = 3
	recFinalize uint16 = 4
	recAbort    uint16 = 5
)

var (
	errJournalClosed  = errors.New("repair session is closed, resume it before any further operation")
	errSimulatedCrash = errors.New("simulated crash")
)

// JournalPath returns the journal path associated with a repaired db path.
func JournalPath(dbPath string) string {
	return dbPath + JournalSuffix
}

// HasJournal reports whether an unfinished repair journal exists for dbPath.
func HasJournal(dbPath string) bool {
	_, err := os.Stat(JournalPath(dbPath))
	return err == nil
}

// journalHeader binds a repair session to one concrete source db version.
type journalHeader struct {
	sessionID  uint64
	pageSize   uint32
	fileSize   uint64
	activeTxid uint64
	metaHash0  [32]byte
	metaHash1  [32]byte
	sourcePath string
	createdAt  time.Time
}

func (h journalHeader) encode() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(journalMagic)
	putBE16(buf, journalVersion)
	putBE64(buf, h.sessionID)
	putBE32(buf, h.pageSize)
	putBE64(buf, h.fileSize)
	putBE64(buf, h.activeTxid)
	buf.Write(h.metaHash0[:])
	buf.Write(h.metaHash1[:])
	putBE64(buf, uint64(h.createdAt.UnixNano()))
	putString(buf, h.sourcePath)
	return buf.Bytes()
}

func decodeHeader(p []byte) (journalHeader, error) {
	r := newReader(p)
	var h journalHeader
	magic, err := r.fixed(len(journalMagic))
	if err != nil {
		return h, err
	}
	if string(magic) != journalMagic {
		return h, fmt.Errorf("invalid journal magic %q", string(magic))
	}
	version, err := r.be16()
	if err != nil {
		return h, err
	}
	if version != journalVersion {
		return h, fmt.Errorf("unsupported journal version %d (expect %d)", version, journalVersion)
	}
	if h.sessionID, err = r.be64(); err != nil {
		return h, err
	}
	ps, err := r.be32()
	if err != nil {
		return h, err
	}
	h.pageSize = uint32(ps)
	if h.fileSize, err = r.be64(); err != nil {
		return h, err
	}
	if h.activeTxid, err = r.be64(); err != nil {
		return h, err
	}
	if h.metaHash0, err = r.hash(); err != nil {
		return h, err
	}
	if h.metaHash1, err = r.hash(); err != nil {
		return h, err
	}
	createdNano, err := r.be64()
	if err != nil {
		return h, err
	}
	h.createdAt = time.Unix(0, int64(createdNano))
	if h.sourcePath, err = r.stringValue(); err != nil {
		return h, err
	}
	return h, nil
}

// applyRecord is a fully prepared (and, when committed, finished) page repair.
type applyRecord struct {
	seq    uint64
	op     string
	pgid   common.Pgid
	before []byte
	after  []byte
}

func (r applyRecord) encode() []byte {
	buf := bytes.NewBuffer(nil)
	putBE64(buf, r.seq)
	putString(buf, r.op)
	putBE64(buf, uint64(r.pgid))
	putBytes(buf, r.before)
	putBytes(buf, r.after)
	return buf.Bytes()
}

func decodeApply(p []byte) (applyRecord, error) {
	r := newReader(p)
	var a applyRecord
	var err error
	if a.seq, err = r.be64(); err != nil {
		return a, err
	}
	if a.op, err = r.stringValue(); err != nil {
		return a, err
	}
	pgid, err := r.be64()
	if err != nil {
		return a, err
	}
	a.pgid = common.Pgid(pgid)
	if a.before, err = r.bytesValue(); err != nil {
		return a, err
	}
	if a.after, err = r.bytesValue(); err != nil {
		return a, err
	}
	return a, nil
}

// Page repair record states as shown by RepairStatus.
const (
	StatePending    = "pending"     // apply logged, neither committed nor aborted
	StateCommitted  = "committed"   // page write finished and verified
	StateRolledBack = "rolled-back" // aborted by recovery, page left as original
)

// PageRepairState describes one recorded page repair.
type PageRepairState struct {
	Seq        uint64
	Op         string
	Pgid       common.Pgid
	State      string
	Committed  bool
	Aborted    bool
	BeforeSize int
	AfterSize  int
	BeforeHash string
	AfterHash  string
}

// RecoveryReport summarizes what Recover did.
type RecoveryReport struct {
	// RolledBack lists uncommitted pages whose bytes were restored from
	// the before-image during this recovery.
	RolledBack []RecoveredPage
	// Aborted lists uncommitted repairs whose abort decision was durably
	// recorded during this recovery (the page is left as original).
	Aborted []RecoveredPage
	// Redone lists committed pages whose on-disk content did not match
	// the after-image and which were rewritten.
	Redone []RecoveredPage
}

// RecoveredPage identifies a page touched during recovery.
type RecoveredPage struct {
	Seq  uint64
	Op   string
	Pgid common.Pgid
}

// RepairStatus is the human-readable state of a repair session.
type RepairStatus struct {
	SessionID      uint64
	SourcePath     string
	PageSize       uint32
	SourceFileSize uint64
	ActiveTxid     uint64
	CreatedAt      time.Time
	Finalized      bool
	Pages          []PageRepairState
}

// RepairSession is an open, journaled repair of one database file.
type RepairSession struct {
	dbPath      string
	journalPath string

	f  *os.File // database file (random read/write)
	jf *os.File // journal file (append-only)

	header    journalHeader
	pageSize  uint64
	fileSize  int64
	records   []applyRecord
	committed map[uint64]bool
	aborted   map[uint64]bool
	finalized bool
	nextSeq   uint64
	closed    bool

	// crashHook is a test seam invoked at crash-sensitive stages
	// ("applyLogged", "pageWritten"). Returning an error aborts the
	// session as if the process had crashed at that point.
	crashHook func(stage string) error
}

// BeginRepair copies srcPath to dstPath (the working copy) and creates a
// journal recording the source version. The source file is never modified.
func BeginRepair(srcPath, dstPath string) (*RepairSession, error) {
	if _, err := os.Stat(srcPath); err != nil {
		return nil, fmt.Errorf("source database file %q: %w", srcPath, err)
	}
	if _, err := os.Stat(dstPath); err == nil {
		return nil, fmt.Errorf("output database file %q already exists", dstPath)
	}
	if HasJournal(dstPath) {
		return nil, fmt.Errorf("a repair journal already exists for %q, use ResumeRepair", dstPath)
	}

	if err := common.CopyFile(srcPath, dstPath); err != nil {
		return nil, fmt.Errorf("copy source database failed: %w", err)
	}
	if err := syncFile(dstPath); err != nil {
		return nil, fmt.Errorf("sync copied database failed: %w", err)
	}
	if err := syncDir(filepath.Dir(dstPath)); err != nil {
		return nil, fmt.Errorf("sync output directory failed: %w", err)
	}

	h, err := buildHeader(srcPath, dstPath)
	if err != nil {
		return nil, err
	}

	jf, err := os.OpenFile(JournalPath(dstPath), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("create journal failed: %w", err)
	}
	if err := writeFrame(jf, recHeader, h.encode()); err != nil {
		_ = jf.Close()
		return nil, fmt.Errorf("write journal header failed: %w", err)
	}
	if err := jf.Sync(); err != nil {
		_ = jf.Close()
		return nil, fmt.Errorf("sync journal header failed: %w", err)
	}
	if err := jf.Close(); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(dstPath)); err != nil {
		return nil, fmt.Errorf("sync journal directory failed: %w", err)
	}

	return openSession(dstPath, false)
}

// OpenRepair opens an existing repair session without running recovery.
// It is intended for read-only inspection (Status, Verify); use
// ResumeRepair to recover interrupted page writes before continuing.
func OpenRepair(dbPath string) (*RepairSession, error) {
	return openSession(dbPath, true)
}

// ResumeRepair opens an existing repair session and runs crash recovery
// immediately: unfinished pages are rolled back, finished but damaged
// pages are redone. The returned report describes the actions taken.
func ResumeRepair(dbPath string) (*RepairSession, *RecoveryReport, error) {
	s, err := openSession(dbPath, true)
	if err != nil {
		return nil, nil, err
	}
	report, err := s.Recover()
	if err != nil {
		_ = s.Close()
		return nil, nil, err
	}
	return s, report, nil
}

func openSession(dbPath string, journalMustExist bool) (*RepairSession, error) {
	f, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open database %q failed: %w", dbPath, err)
	}

	jpath := JournalPath(dbPath)
	jraw, err := os.ReadFile(jpath)
	if err != nil {
		_ = f.Close()
		if os.IsNotExist(err) && journalMustExist {
			return nil, fmt.Errorf("no repair journal found for %q, begin a repair first", dbPath)
		}
		return nil, fmt.Errorf("read journal %q failed: %w", jpath, err)
	}

	h, records, committed, aborted, finalized, err := parseJournal(jraw)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("parse journal %q failed: %w", jpath, err)
	}

	jf, err := os.OpenFile(jpath, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open journal %q failed: %w", jpath, err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = jf.Close()
		return nil, err
	}

	s := &RepairSession{
		dbPath:      dbPath,
		journalPath: jpath,
		f:           f,
		jf:          jf,
		header:      h,
		pageSize:    uint64(h.pageSize),
		fileSize:    fi.Size(),
		records:     records,
		committed:   committed,
		aborted:     aborted,
		finalized:   finalized,
		nextSeq:     1,
	}
	for _, r := range records {
		if r.seq >= s.nextSeq {
			s.nextSeq = r.seq + 1
		}
	}
	return s, nil
}

// buildHeader gathers the version identity of the freshly copied database.
func buildHeader(srcPath, dstPath string) (journalHeader, error) {
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(dstPath)
	if err != nil {
		return journalHeader{}, fmt.Errorf("read page size failed: %w", err)
	}
	meta, _, err := guts_cli.GetActiveMetaPage(dstPath)
	if err != nil {
		return journalHeader{}, fmt.Errorf("read active meta page failed: %w", err)
	}

	f, err := os.Open(dstPath)
	if err != nil {
		return journalHeader{}, fmt.Errorf("open output database failed: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return journalHeader{}, err
	}
	h0 := make([]byte, pageSize)
	h1 := make([]byte, pageSize)
	if _, err := f.ReadAt(h0, 0); err != nil {
		return journalHeader{}, fmt.Errorf("read meta page 0 failed: %w", err)
	}
	if _, err := f.ReadAt(h1, int64(pageSize)); err != nil {
		return journalHeader{}, fmt.Errorf("read meta page 1 failed: %w", err)
	}

	var sid [8]byte
	if _, err := rand.Read(sid[:]); err != nil {
		return journalHeader{}, err
	}
	return journalHeader{
		sessionID:  binary.BigEndian.Uint64(sid[:]),
		pageSize:   uint32(pageSize),
		fileSize:   uint64(fi.Size()),
		activeTxid: uint64(meta.Txid()),
		metaHash0:  sha256.Sum256(h0),
		metaHash1:  sha256.Sum256(h1),
		sourcePath: srcPath,
		createdAt:  time.Now(),
	}, nil
}

// ApplyPage records and performs a page replacement. The caller supplies
// the full new page span (page header + data, including overflow pages)
// in after; the current on-disk bytes are captured as before-image.
//
// Ordering (crash safe):
//  1. append APPLY record (before+after) and fsync the journal;
//  2. write the page, fsync the database and read it back;
//  3. append COMMIT and fsync the journal.
func (s *RepairSession) ApplyPage(op string, pgid common.Pgid, after []byte) error {
	if s.closed {
		return errJournalClosed
	}
	if s.finalized {
		return fmt.Errorf("repair session is already finalized")
	}
	if op == "" {
		return fmt.Errorf("operation name must not be empty")
	}

	// Validate the after-image: length matches declared overflow span
	// and the page self-identifies as the target page.
	p := common.LoadPage(after)
	expectedLen := s.pageSize * (uint64(p.Overflow()) + 1)
	if uint64(len(after)) != expectedLen {
		return fmt.Errorf("after-image length %d != pageSize*(overflow+1) %d", len(after), expectedLen)
	}
	if p.Id() != pgid {
		return fmt.Errorf("after-image page id %d != target page id %d", p.Id(), pgid)
	}

	// Capture the full current span (including overflow pages).
	oldSpan, err := s.currentSpan(pgid)
	if err != nil {
		return fmt.Errorf("capture before-image of page %d failed: %w", pgid, err)
	}
	before := make([]byte, oldSpan)
	if err := readFullAt(s.f, before, int64(pgid)*int64(s.pageSize)); err != nil {
		return fmt.Errorf("read before-image of page %d failed: %w", pgid, err)
	}
	if bytes.Equal(before, after) {
		return fmt.Errorf("page %d already contains the requested content, nothing to repair", pgid)
	}

	rec := applyRecord{
		seq:    s.nextSeq,
		op:     op,
		pgid:   pgid,
		before: before,
		after:  after,
	}

	// 1. Journal the intent (with both images), durable first.
	if err := writeFrame(s.jf, recApply, rec.encode()); err != nil {
		return fmt.Errorf("journal apply record failed: %w", err)
	}
	if err := s.jf.Sync(); err != nil {
		return fmt.Errorf("sync journal apply record failed: %w", err)
	}
	if s.crashHook != nil {
		if err := s.crashHook("applyLogged"); err != nil {
			s.closed = true
			return fmt.Errorf("crash after apply record: %w", errors.Join(err, errSimulatedCrash))
		}
	}

	// 2. Apply the page durably, then read it back.
	if err := s.writePageSpan(pgid, after); err != nil {
		return fmt.Errorf("write page %d failed: %w", pgid, err)
	}
	if s.crashHook != nil {
		if err := s.crashHook("pageWritten"); err != nil {
			s.closed = true
			return fmt.Errorf("crash after page write: %w", errors.Join(err, errSimulatedCrash))
		}
	}
	current := make([]byte, len(after))
	if err := readFullAt(s.f, current, int64(pgid)*int64(s.pageSize)); err != nil {
		return fmt.Errorf("read back page %d failed: %w", pgid, err)
	}
	if !bytes.Equal(current, after) {
		// Uncommitted; Recover will roll the before-image back.
		return fmt.Errorf("page %d read-back mismatch: disk does not match after-image", pgid)
	}

	// 3. Commit the record.
	if err := s.appendMarker(recCommit, rec.seq); err != nil {
		return fmt.Errorf("journal commit record failed: %w", err)
	}

	s.records = append(s.records, rec)
	s.committed[rec.seq] = true
	s.nextSeq++
	return nil
}

// CopyPage journals and performs a copy of srcPage content to target page,
// mirroring the standalone CopyPage semantics but crash safely.
func (s *RepairSession) CopyPage(srcPage, target common.Pgid) error {
	_, data, err := guts_cli.ReadPage(s.dbPath, uint64(srcPage))
	if err != nil {
		return fmt.Errorf("read source page %d failed: %w", srcPage, err)
	}
	common.LoadPage(data).SetId(target)
	return s.ApplyPage("copy-page", target, data)
}

// Recover replays the journal against the database.
//
// Committed pages not matching their after-image are redone; uncommitted
// pages get their before-image restored and an abort marker recorded. It
// is idempotent.
func (s *RepairSession) Recover() (*RecoveryReport, error) {
	if s.closed {
		return nil, errJournalClosed
	}
	report := &RecoveryReport{}
	for _, r := range s.records {
		offset := int64(r.pgid) * int64(s.pageSize)
		switch {
		case s.committed[r.seq]:
			current := make([]byte, len(r.after))
			if err := readFullAt(s.f, current, offset); err != nil {
				return nil, fmt.Errorf("recover page %d: read failed: %w", r.pgid, err)
			}
			if bytes.Equal(current, r.after) {
				continue // finished page, confirmed intact
			}
			if err := s.writePageSpan(r.pgid, r.after); err != nil {
				return nil, fmt.Errorf("redo page %d failed: %w", r.pgid, err)
			}
			report.Redone = append(report.Redone, RecoveredPage{Seq: r.seq, Op: r.op, Pgid: r.pgid})

		case s.aborted[r.seq]:
			// A previous recovery decided to abort this repair. Make sure
			// the rollback itself was not interrupted by another crash.
			current := make([]byte, len(r.before))
			if err := readFullAt(s.f, current, offset); err != nil {
				return nil, fmt.Errorf("recover page %d: read failed: %w", r.pgid, err)
			}
			if bytes.Equal(current, r.before) {
				continue
			}
			if err := s.writePageSpan(r.pgid, r.before); err != nil {
				return nil, fmt.Errorf("finish rollback of page %d failed: %w", r.pgid, err)
			}
			report.RolledBack = append(report.RolledBack, RecoveredPage{Seq: r.seq, Op: r.op, Pgid: r.pgid})

		default:
			// Repair died without commit: restore original bytes, then
			// durably record the abort decision.
			current := make([]byte, len(r.before))
			needRewrite := true
			if err := readFullAt(s.f, current, offset); err == nil && bytes.Equal(current, r.before) {
				needRewrite = false
			}
			if needRewrite {
				if err := s.writePageSpan(r.pgid, r.before); err != nil {
					return nil, fmt.Errorf("rollback page %d failed: %w", r.pgid, err)
				}
				report.RolledBack = append(report.RolledBack, RecoveredPage{Seq: r.seq, Op: r.op, Pgid: r.pgid})
			}
			if err := s.appendMarker(recAbort, r.seq); err != nil {
				return nil, fmt.Errorf("record abort of page %d failed: %w", r.pgid, err)
			}
			s.aborted[r.seq] = true
			report.Aborted = append(report.Aborted, RecoveredPage{Seq: r.seq, Op: r.op, Pgid: r.pgid})
		}
	}
	return report, nil
}

// appendMarker appends a commit/abort marker for seq and fsyncs it.
func (s *RepairSession) appendMarker(recType uint16, seq uint64) error {
	buf := bytes.NewBuffer(nil)
	putBE64(buf, seq)
	if err := writeFrame(s.jf, recType, buf.Bytes()); err != nil {
		return err
	}
	return s.jf.Sync()
}

// Verify confirms that the on-disk file still matches the repair:
//   - the source version identity (meta pages, file size) is unchanged;
//   - every repaired page still equals its committed after-image;
//   - no uncommitted (half-modified) repair is pending.
func (s *RepairSession) Verify() error {
	if s.closed {
		return errJournalClosed
	}
	var problems []string

	// Version binding.
	fi, err := s.f.Stat()
	if err != nil {
		return err
	}
	if uint64(fi.Size()) != s.header.fileSize {
		problems = append(problems, fmt.Sprintf(
			"file size %d differs from the repaired version %d", fi.Size(), s.header.fileSize))
	}
	h0 := make([]byte, s.pageSize)
	h1 := make([]byte, s.pageSize)
	if err := readFullAt(s.f, h0, 0); err != nil {
		return fmt.Errorf("read meta page 0 failed: %w", err)
	}
	if err := readFullAt(s.f, h1, int64(s.pageSize)); err != nil {
		return fmt.Errorf("read meta page 1 failed: %w", err)
	}
	if sha256.Sum256(h0) != s.header.metaHash0 || sha256.Sum256(h1) != s.header.metaHash1 {
		problems = append(problems, fmt.Sprintf(
			"meta pages no longer match the repaired version (txid=%d)", s.header.activeTxid))
	}

	// Only the latest record for a page describes its intended state.
	latest := make(map[common.Pgid]applyRecord)
	for _, r := range s.records {
		latest[r.pgid] = r
	}
	for _, r := range latest {
		offset := int64(r.pgid) * int64(s.pageSize)
		switch {
		case s.committed[r.seq]:
			current := make([]byte, len(r.after))
			if err := readFullAt(s.f, current, offset); err != nil {
				problems = append(problems, fmt.Sprintf("page %d unreadable: %v", r.pgid, err))
				continue
			}
			if !bytes.Equal(current, r.after) {
				problems = append(problems, fmt.Sprintf(
					"page %d (%s) content does not match the committed after-image", r.pgid, r.op))
			}
		case s.aborted[r.seq]:
			// Repair was given up: the page must be exactly as original.
			current := make([]byte, len(r.before))
			if err := readFullAt(s.f, current, offset); err != nil {
				problems = append(problems, fmt.Sprintf("page %d unreadable: %v", r.pgid, err))
				continue
			}
			if !bytes.Equal(current, r.before) {
				problems = append(problems, fmt.Sprintf(
					"page %d (%s) was rolled back but differs from the recorded before-image",
					r.pgid, r.op))
			}
		default:
			problems = append(problems, fmt.Sprintf(
				"page %d (%s, seq=%d) repair is not committed, run recover to finish or abort it",
				r.pgid, r.op, r.seq))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("repair verification failed:\n  - %s", joinLines(problems))
	}
	return nil
}

// Status returns the recorded state of the repair.
func (s *RepairSession) Status() RepairStatus {
	st := RepairStatus{
		SessionID:      s.header.sessionID,
		SourcePath:     s.header.sourcePath,
		PageSize:       s.header.pageSize,
		SourceFileSize: s.header.fileSize,
		ActiveTxid:     s.header.activeTxid,
		CreatedAt:      s.header.createdAt,
		Finalized:      s.finalized,
		Pages:          make([]PageRepairState, 0, len(s.records)),
	}
	for _, r := range s.records {
		state := StatePending
		switch {
		case s.committed[r.seq]:
			state = StateCommitted
		case s.aborted[r.seq]:
			state = StateRolledBack
		}
		st.Pages = append(st.Pages, PageRepairState{
			Seq:        r.seq,
			Op:         r.op,
			Pgid:       r.pgid,
			State:      state,
			Committed:  s.committed[r.seq],
			Aborted:    s.aborted[r.seq],
			BeforeSize: len(r.before),
			AfterSize:  len(r.after),
			BeforeHash: shortHash(r.before),
			AfterHash:  shortHash(r.after),
		})
	}
	return st
}

// Finalize verifies the repair and writes the finalization marker.
// After finalization no more pages can be applied; call Finish to remove
// the journal and release the file.
func (s *RepairSession) Finalize() error {
	if s.closed {
		return errJournalClosed
	}
	if s.finalized {
		return nil
	}
	if err := s.Verify(); err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	putBE64(buf, uint64(time.Now().UnixNano()))
	putBE32(buf, uint32(len(s.records)))
	if err := writeFrame(s.jf, recFinalize, buf.Bytes()); err != nil {
		return fmt.Errorf("journal finalize record failed: %w", err)
	}
	if err := s.jf.Sync(); err != nil {
		return fmt.Errorf("sync journal finalize record failed: %w", err)
	}
	s.finalized = true
	return nil
}

// Finish verifies & finalizes the repair, then removes the journal. The
// repaired database is left as a plain file once this succeeds.
func (s *RepairSession) Finish() error {
	if err := s.Finalize(); err != nil {
		return err
	}
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.journalPath); err != nil {
		return fmt.Errorf("remove journal failed: %w", err)
	}
	return syncDir(filepath.Dir(s.dbPath))
}

// Close releases the database and journal file handles.
func (s *RepairSession) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err1, err2 error
	if s.f != nil {
		err1 = s.f.Close()
	}
	if s.jf != nil {
		err2 = s.jf.Close()
	}
	return errors.Join(err1, err2)
}

// currentSpan determines the current on-disk span (page + overflow) of pgid.
func (s *RepairSession) currentSpan(pgid common.Pgid) (uint64, error) {
	offset := int64(pgid) * int64(s.pageSize)
	if offset < 0 || offset+int64(common.PageHeaderSize) > s.fileSize {
		return 0, fmt.Errorf("page %d is outside of the file", pgid)
	}
	hdr := make([]byte, int(common.PageHeaderSize))
	if err := readFullAt(s.f, hdr, offset); err != nil {
		return 0, err
	}
	p := common.LoadPage(hdr)
	if p.Id() != pgid {
		return 0, fmt.Errorf("page self-identifies as %d, expected %d", p.Id(), pgid)
	}
	span := s.pageSize * (uint64(p.Overflow()) + 1)
	if uint64(s.fileSize)-uint64(offset) < span {
		return 0, fmt.Errorf("page declares %d overflow pages beyond end of file", p.Overflow())
	}
	return span, nil
}

// writePageSpan writes the span at the page offset, fsyncs and verifies it.
func (s *RepairSession) writePageSpan(pgid common.Pgid, data []byte) error {
	offset := int64(pgid) * int64(s.pageSize)
	if uint64(s.fileSize)-uint64(offset) < uint64(len(data)) {
		return fmt.Errorf("page span (%d bytes) exceeds file size", len(data))
	}
	if _, err := s.f.WriteAt(data, offset); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	readBack := make([]byte, len(data))
	if err := readFullAt(s.f, readBack, offset); err != nil {
		return err
	}
	if !bytes.Equal(readBack, data) {
		return fmt.Errorf("read-back verification failed for page %d", pgid)
	}
	return nil
}

// ---- journal file framing ----

// frame layout: [type uint16][length uint32][payload][crc32 of payload].
func writeFrame(w io.Writer, recType uint16, payload []byte) error {
	frame := make([]byte, 6+len(payload)+4)
	binary.BigEndian.PutUint16(frame[0:2], recType)
	binary.BigEndian.PutUint32(frame[2:6], uint32(len(payload)))
	copy(frame[6:], payload)
	binary.BigEndian.PutUint32(frame[6+len(payload):], crc32.ChecksumIEEE(payload))
	_, err := w.Write(frame)
	return err
}

// parseJournal reads all complete frames. A truncated or CRC-invalid tail
// frame (crash while appending) is ignored: its database write could not
// have started, because the database write happens only after the frame
// was fully fsynced.
func parseJournal(data []byte) (journalHeader, []applyRecord, map[uint64]bool, map[uint64]bool, bool, error) {
	var (
		header    journalHeader
		records   []applyRecord
		committed = make(map[uint64]bool)
		aborted   = make(map[uint64]bool)
		finalized bool
		off       int
	)
	haveHeader := false
	for {
		if off == len(data) {
			break
		}
		if len(data)-off < 6 {
			break // torn frame header
		}
		recType := binary.BigEndian.Uint16(data[off : off+2])
		length := int(binary.BigEndian.Uint32(data[off+2 : off+6]))
		end := off + 6 + length + 4
		if end > len(data) {
			break // torn frame body
		}
		payload := data[off+6 : off+6+length]
		gotCRC := binary.BigEndian.Uint32(data[off+6+length : end])
		if crc32.ChecksumIEEE(payload) != gotCRC {
			break // torn or corrupted frame
		}
		off = end

		switch recType {
		case recHeader:
			if haveHeader {
				return header, nil, nil, nil, false, fmt.Errorf("duplicate journal header")
			}
			h, err := decodeHeader(payload)
			if err != nil {
				return header, nil, nil, nil, false, err
			}
			header = h
			haveHeader = true
		case recApply:
			if !haveHeader {
				return header, nil, nil, nil, false, fmt.Errorf("apply record before header")
			}
			rec, err := decodeApply(payload)
			if err != nil {
				return header, nil, nil, nil, false, err
			}
			records = append(records, rec)
		case recCommit, recAbort:
			if len(payload) < 8 {
				return header, nil, nil, nil, false, fmt.Errorf("short marker record")
			}
			seq := binary.BigEndian.Uint64(payload[:8])
			if recType == recCommit {
				committed[seq] = true
			} else {
				aborted[seq] = true
			}
		case recFinalize:
			finalized = true
		default:
			return header, nil, nil, nil, false, fmt.Errorf("unknown journal record type %d", recType)
		}
	}
	if !haveHeader {
		return header, nil, nil, nil, false, fmt.Errorf("journal has no valid header")
	}
	return header, records, committed, aborted, finalized, nil
}

// ---- small encoding helpers ----

type beReader struct {
	data []byte
	pos  int
}

func newReader(data []byte) *beReader { return &beReader{data: data} }

func (r *beReader) fixed(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *beReader) be16() (uint16, error) {
	b, err := r.fixed(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *beReader) be32() (uint32, error) {
	b, err := r.fixed(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *beReader) be64() (uint64, error) {
	b, err := r.fixed(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *beReader) hash() ([32]byte, error) {
	var h [32]byte
	b, err := r.fixed(32)
	if err != nil {
		return h, err
	}
	copy(h[:], b)
	return h, nil
}

func (r *beReader) stringValue() (string, error) {
	b, err := r.bytesValue()
	return string(b), err
}

func (r *beReader) bytesValue() ([]byte, error) {
	n, err := r.be32()
	if err != nil {
		return nil, err
	}
	return r.fixed(int(n))
}

func putBE16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func putBE32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func putBE64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

func putString(buf *bytes.Buffer, s string) {
	putBytes(buf, []byte(s))
}

func putBytes(buf *bytes.Buffer, b []byte) {
	putBE32(buf, uint32(len(b)))
	buf.Write(b)
}

func readFullAt(f *os.File, buf []byte, off int64) error {
	n, err := f.ReadAt(buf, off)
	if err == io.EOF && n == len(buf) {
		return nil
	}
	if err != nil && err != io.EOF {
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(dir string) error {
	if dir == "" {
		dir = "."
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func shortHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n  - "
		}
		out += l
	}
	return out
}
