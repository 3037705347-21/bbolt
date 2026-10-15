package surgeon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Journal status values. They are persisted; treat as a stable API.
const (
	statusDecided   = "decided"
	statusApplied   = "applied"
	statusCompleted = "completed"
	statusAborted   = "aborted"
)

// Journal event types.
const (
	eventPhase    = "phase"
	eventRollback = "rollback"
	eventError    = "error"
	eventInfo     = "info"
)

const journalSchemaVersion = 1

// recoveryJournal is the on-disk decision record. Only the current episode
// is acted upon; previous episodes are preserved verbatim in History.
type recoveryJournal struct {
	SchemaVersion int               `json:"schemaVersion"`
	Current       *journalEpisode   `json:"current,omitempty"`
	History       []*journalEpisode `json:"history,omitempty"`
}

// journalEpisode captures one recovery decision for one specific source
// state (identified by SourceSize + PreFingerprints).
type journalEpisode struct {
	StartedAt  string `json:"startedAt"`
	Status     string `json:"status"`
	SourcePath string `json:"sourcePath"`
	OutputPath string `json:"outputPath"`

	SourceSize int64  `json:"sourceSize"`
	PageSize   uint32 `json:"pageSize"`
	// PreFingerprints identify the source meta pages this decision was
	// made for.
	PreFingerprints [metaPageCount]string `json:"preFingerprints"`
	// ExpectedPostFingerprints identify the output meta pages after the
	// recorded action has been applied.
	ExpectedPostFingerprints [metaPageCount]string `json:"expectedPostFingerprints"`

	Inspections [metaPageCount]MetaPageInspection `json:"inspections"`
	Decision    Decision                          `json:"decision"`

	// Backups are the copies of the original meta pages taken before any
	// repair write.
	Backups []string `json:"backups,omitempty"`

	AbortError  string         `json:"abortError,omitempty"`
	Events      []journalEvent `json:"events"`
	CompletedAt string         `json:"completedAt,omitempty"`
}

// journalEvent is an append-only, timestamped audit entry.
type journalEvent struct {
	Time    string `json:"time"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func (e *journalEpisode) appendEvent(typ, message string) {
	e.Events = append(e.Events, journalEvent{Time: nowRFC3339(), Type: typ, Message: message})
}

// matchesSource reports whether the episode was decided for exactly the
// source state identified by size and the two pre-repair fingerprints.
func (e *journalEpisode) matchesSource(size int64, fingerprints [metaPageCount]string) bool {
	if e == nil || e.SourceSize != size {
		return false
	}
	return e.PreFingerprints == fingerprints
}

// loadJournal reads and parses the journal. A missing file yields an empty
// journal and exists=false. A malformed journal is a hard error: the old
// decision record must never be silently overwritten.
func loadJournal(path string) (j *recoveryJournal, exists bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &recoveryJournal{SchemaVersion: journalSchemaVersion}, false, nil
		}
		return nil, false, fmt.Errorf("read recovery journal %q: %w", path, err)
	}
	j = &recoveryJournal{}
	if err := json.Unmarshal(data, j); err != nil {
		return nil, true, fmt.Errorf("parse recovery journal %q (refusing to overwrite a corrupted decision record): %w", path, err)
	}
	if j.SchemaVersion == 0 {
		j.SchemaVersion = journalSchemaVersion
	}
	return j, true, nil
}

// saveJournalAtomic persists the journal through a same-directory temp file
// that is fsynced and renamed, so an interrupted process never leaves a
// half-written journal at the final path.
func saveJournalAtomic(path string, j *recoveryJournal) (err error) {
	if j.SchemaVersion == 0 {
		j.SchemaVersion = journalSchemaVersion
	}
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return fmt.Errorf("encode recovery journal: %w", err)
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmpPath := filepath.Join(dir, "."+base+".tmp")

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create journal temp file %q: %w", tmpPath, err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
		}
	}()
	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("write journal temp file %q: %w", tmpPath, err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("sync journal temp file %q: %w", tmpPath, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close journal temp file %q: %w", tmpPath, err)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename journal temp file to %q: %w", path, err)
	}
	syncDir(dir)
	return nil
}

// stageTransition appends a phase event and persists the new status.
func stageTransition(j *recoveryJournal, path, status, message string) error {
	j.Current.Status = status
	j.Current.appendEvent(eventPhase, message)
	if status == statusCompleted {
		j.Current.CompletedAt = nowRFC3339()
	}
	return saveJournalAtomic(path, j)
}

// beginEpisode archives the current episode (if any) into History and makes
// the given episode the current one.
func (j *recoveryJournal) beginEpisode(ep *journalEpisode) {
	if j.Current != nil {
		j.History = append(j.History, j.Current)
	}
	j.Current = ep
}

// syncDir fsyncs a directory so that a rename is durable after a crash.
// Directory fsync is not supported on Windows.
func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}
