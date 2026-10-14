package bbolt

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"path/filepath"

	berrors "go.etcd.io/bbolt/errors"
)

const (
	// resumableCompactCheckpointVersion is the on-disk format version of the
	// checkpoint sidecar file.
	resumableCompactCheckpointVersion = 1

	// defaultResumableCompactBatchSize is the amount of source data (bytes)
	// copied into one destination transaction before a durable checkpoint is
	// written when the caller does not specify a batch size.
	defaultResumableCompactBatchSize = int64(64 << 20)

	resumableCompactPartialSuffix    = ".compact-partial"
	resumableCompactCheckpointSuffix = ".compact-checkpoint"
	resumableCompactSidecarTmpSuffix = ".tmp"

	resumableCompactStatusCopying = "copying"
	resumableCompactStatusStaged  = "staged"
)

// ResumableCompactOptions are used to configure CompactResumable.
type ResumableCompactOptions struct {
	// BatchSize limits the amount of source data (in bytes) copied into the
	// destination database between two durable checkpoints. It also bounds the
	// size of individual write transactions. A value <= 0 selects the default
	// of 64 MiB.
	BatchSize int64

	// Restart discards any previous partial output and checkpoint for the
	// destination path and compacts the source from scratch. The final
	// destination file is never removed by this option.
	Restart bool

	// Logger, if set, receives progress messages. When nil, messages are
	// discarded.
	Logger Logger

	// interrupt, if non-nil, is invoked at points where all progress has been
	// synced so that tests can simulate a crash or power failure. Returning an
	// error aborts the run; the durable state on disk allows a subsequent call
	// to resume. Stages are "checkpoint" (after a batch was synced), "staged"
	// (after the full output was verified, before it is published) and
	// "renamed" (after the output was published, before the checkpoint is
	// removed).
	interrupt func(stage string, batches int) error
}

// CompactResumable creates a compacted copy of the bbolt database at srcPath at
// dstPath, persisting progress after each batch so that an interrupted run can
// be resumed.
//
// Call CompactResumable again with the same srcPath/dstPath after an
// interruption: copying continues from the last durable checkpoint instead of
// starting over.
//
// The destination is built in a temporary file (dstPath + ".compact-partial")
// next to dstPath. dstPath is created atomically (via rename) only after the
// entire source snapshot has been copied and verified to be logically
// identical to the source; dstPath is never left containing a partial result.
//
// The source snapshot is pinned by a read-only transaction for the duration of
// a run. When a run resumes, the source database is checked against the
// fingerprint recorded in the checkpoint (dstPath + ".compact-checkpoint"). If
// the source has been modified since the interrupted run,
// go.etcd.io/bbolt/errors.ErrCompactSourceChanged is returned and the partial
// output is discarded, because data from different source snapshots must not
// be mixed. A subsequent call then performs a fresh compaction of the current
// source.
func CompactResumable(srcPath, dstPath string, mode os.FileMode, opts *ResumableCompactOptions) error {
	if opts == nil {
		opts = &ResumableCompactOptions{}
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = defaultResumableCompactBatchSize
	}
	logger := opts.Logger
	if logger == nil {
		logger = getDiscardLogger()
	}

	absSrc, err := filepath.Abs(srcPath)
	if err != nil {
		return fmt.Errorf("compact: cannot resolve source path: %w", err)
	}
	absDst, err := filepath.Abs(dstPath)
	if err != nil {
		return fmt.Errorf("compact: cannot resolve destination path: %w", err)
	}
	if absSrc == absDst {
		return fmt.Errorf("compact: source and destination paths must differ")
	}

	partialPath := absDst + resumableCompactPartialSuffix
	sidecarPath := absDst + resumableCompactCheckpointSuffix
	tmpSidecarPath := sidecarPath + resumableCompactSidecarTmpSuffix

	// Never operate on directories masquerading as our files.
	for _, path := range []string{absDst, partialPath, sidecarPath} {
		if fi, statErr := os.Stat(path); statErr == nil && fi.IsDir() {
			return fmt.Errorf("compact: %s is a directory", path)
		}
	}

	if opts.Restart {
		// Only discard artifacts produced by this feature, never dstPath.
		for _, path := range []string{partialPath, sidecarPath, tmpSidecarPath} {
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				return rmErr
			}
		}
	} else if rmErr := os.Remove(tmpSidecarPath); rmErr != nil && !os.IsNotExist(rmErr) {
		// A leftover temp sidecar can never be authoritative: the rename of
		// the temp file onto the sidecar path is the commit point.
		return rmErr
	}

	cp, cpExists, err := loadResumableCompactCheckpoint(sidecarPath)
	if err != nil {
		return err
	}

	dstExists, err := regularFileExists(absDst)
	if err != nil {
		return err
	}
	partialExists, err := regularFileExists(partialPath)
	if err != nil {
		return err
	}

	// A staged checkpoint marks a fully copied, verified output. Recovery from
	// this state never needs the source database: the recorded content hash is
	// enough to verify the file.
	if cpExists && cp.Status == resumableCompactStatusStaged {
		switch {
		case dstExists:
			return recoverStagedDestination(absDst, partialPath, sidecarPath, tmpSidecarPath, cp, logger)
		case partialExists:
			return finalizeStagedPartial(absDst, partialPath, sidecarPath, tmpSidecarPath, cp, logger, opts.interrupt)
		default:
			return fmt.Errorf("%w: staged checkpoint without destination or partial output", berrors.ErrCompactCheckpointCorrupt)
		}
	}
	if dstExists {
		return berrors.ErrCompactOutputExists
	}
	if !cpExists && partialExists {
		return fmt.Errorf("%w: %s", berrors.ErrCompactPartialExists, partialPath)
	}
	if cpExists && !partialExists {
		return fmt.Errorf("%w: checkpoint references missing partial output %s", berrors.ErrCompactCheckpointCorrupt, partialPath)
	}

	// Open the source read-only. A shared lock is taken, so other processes
	// may keep writing; the read transaction below pins a consistent snapshot.
	src, err := Open(absSrc, 0, &Options{ReadOnly: true, Logger: logger})
	if err != nil {
		return fmt.Errorf("compact: cannot open source database: %w", err)
	}
	defer func() { _ = src.Close() }()

	var (
		dst              *DB
		discardArtifacts bool
		viewErr          error
	)
	viewErr = src.View(func(srcTx *Tx) error {
		fp := captureCompactFingerprint(srcTx, absSrc)

		if cpExists {
			if cp.SourcePath != absSrc || !cp.Fingerprint.equal(fp) {
				discardArtifacts = true
				return berrors.ErrCompactSourceChanged
			}
		} else {
			cp = &resumableCompactCheckpoint{
				Version:     resumableCompactCheckpointVersion,
				Status:      resumableCompactStatusCopying,
				SourcePath:  absSrc,
				Fingerprint: fp,
			}
		}

		// Open (resume) or create (fresh run) the partial destination. It is
		// always opened with syncing enabled: durable checkpoints depend on
		// committed transactions being on stable storage.
		dst, err = Open(partialPath, mode, &Options{Logger: logger})
		if err != nil {
			return fmt.Errorf("compact: cannot open partial output: %w", err)
		}

		if !cpExists {
			if err := cp.write(sidecarPath, tmpSidecarPath); err != nil {
				return err
			}
			cpExists = true
			logger.Infof("compact: started, checkpoints written to %s", sidecarPath)
		} else {
			logger.Infof("compact: resuming from checkpoint at batch %d (%d items, %d bytes copied)",
				cp.Batches, cp.Position.Items, cp.Position.Bytes)
		}

		if cp.Status != resumableCompactStatusCopying {
			return fmt.Errorf("%w: unexpected checkpoint status %q", berrors.ErrCompactCheckpointCorrupt, cp.Status)
		}
		if err := copyWithCheckpoints(srcTx, dst, cp, batchSize, sidecarPath, tmpSidecarPath, logger, opts.interrupt); err != nil {
			return err
		}

		// Reconcile the full partial output against the pinned source snapshot
		// and run bbolt's own consistency check before publishing anything.
		sum, err := verifyCompaction(srcTx, dst)
		if err != nil {
			return err
		}

		cp.Status = resumableCompactStatusStaged
		cp.ContentHash = fmt.Sprintf("%016x", sum)
		if err := cp.write(sidecarPath, tmpSidecarPath); err != nil {
			return err
		}
		logger.Infof("compact: output verified and staged")

		if opts.interrupt != nil {
			if err := opts.interrupt("staged", cp.Batches); err != nil {
				return err
			}
		}
		return nil
	})

	if dst != nil {
		if closeErr := dst.Close(); closeErr != nil && viewErr == nil {
			viewErr = closeErr
		}
	}

	if viewErr != nil {
		if discardArtifacts {
			logger.Warningf("compact: source database changed since the checkpoint; discarding partial output %s", partialPath)
			_ = os.Remove(partialPath)
			_ = os.Remove(sidecarPath)
			_ = os.Remove(tmpSidecarPath)
			_ = fsyncDir(filepath.Dir(absDst))
		}
		return viewErr
	}

	// Publish the verified output atomically and only then drop the checkpoint.
	if err := os.Rename(partialPath, absDst); err != nil {
		return fmt.Errorf("compact: failed to publish destination: %w", err)
	}
	if err := fsyncDir(filepath.Dir(absDst)); err != nil {
		logger.Warningf("compact: fsync of destination directory failed: %v", err)
	}

	if opts.interrupt != nil {
		if err := opts.interrupt("renamed", cp.Batches); err != nil {
			// dstPath already contains a complete, verified database; the
			// leftover checkpoint lets the next call detect exactly that.
			return err
		}
	}

	if err := os.Remove(sidecarPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(tmpSidecarPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := fsyncDir(filepath.Dir(absDst)); err != nil {
		logger.Warningf("compact: fsync of destination directory failed: %v", err)
	}
	logger.Infof("compact: published compacted database at %s", absDst)
	return nil
}

// resumableCompactCheckpoint is the JSON document persisted next to the
// destination while a resumable compaction is in progress.
type resumableCompactCheckpoint struct {
	Version     int                         `json:"version"`
	Status      string                      `json:"status"`
	SourcePath  string                      `json:"source_path"`
	Fingerprint resumableCompactFingerprint `json:"fingerprint"`
	Position    resumableCompactPosition    `json:"position"`
	ContentHash string                      `json:"content_hash,omitempty"`
	Batches     int                         `json:"batches"`
}

// resumableCompactFingerprint identifies the exact source snapshot a run is
// copying. It is captured from the read transaction used for the walk.
type resumableCompactFingerprint struct {
	Txid     uint64 `json:"txid"`
	Pgid     uint64 `json:"pgid"`
	RootPgid uint64 `json:"root_pgid"`
	PageSize uint32 `json:"page_size"`
	Checksum uint64 `json:"checksum"`
	FileSize int64  `json:"file_size"`
}

func (f resumableCompactFingerprint) equal(other resumableCompactFingerprint) bool {
	return f == other
}

func captureCompactFingerprint(tx *Tx, srcPath string) resumableCompactFingerprint {
	var fileSize int64
	if fi, err := os.Stat(srcPath); err == nil {
		fileSize = fi.Size()
	}
	m := tx.meta
	return resumableCompactFingerprint{
		Txid:     uint64(m.Txid()),
		Pgid:     uint64(m.Pgid()),
		RootPgid: uint64(m.RootBucket().RootPage()),
		PageSize: m.PageSize(),
		Checksum: m.Checksum(),
		FileSize: fileSize,
	}
}

// resumableCompactPosition is the resumable DFS frontier: Path holds the bucket
// names leading from the root to the bucket being iterated and Key is the last
// fully processed direct child of that bucket. When KeyBucketDone is true, Key
// denotes a bucket whose whole subtree has already been walked; otherwise Key
// denotes a copied key/value pair that is safe to copy again idempotently.
type resumableCompactPosition struct {
	Path          [][]byte
	Key           []byte
	KeyBucketDone bool
	Items         int64
	Bytes         int64
}

type compactPositionDTO struct {
	Path          []string `json:"path,omitempty"`
	Key           string   `json:"key,omitempty"`
	KeyBucketDone bool     `json:"key_bucket_done,omitempty"`
	Items         int64    `json:"items"`
	Bytes         int64    `json:"bytes"`
}

func (p resumableCompactPosition) MarshalJSON() ([]byte, error) {
	dto := compactPositionDTO{
		KeyBucketDone: p.KeyBucketDone,
		Items:         p.Items,
		Bytes:         p.Bytes,
	}
	for _, k := range p.Path {
		dto.Path = append(dto.Path, base64.StdEncoding.EncodeToString(k))
	}
	if p.Key != nil {
		dto.Key = base64.StdEncoding.EncodeToString(p.Key)
	}
	return json.Marshal(dto)
}

func (p *resumableCompactPosition) UnmarshalJSON(data []byte) error {
	var dto compactPositionDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}
	*p = resumableCompactPosition{
		KeyBucketDone: dto.KeyBucketDone,
		Items:         dto.Items,
		Bytes:         dto.Bytes,
	}
	for _, enc := range dto.Path {
		k, err := base64.StdEncoding.DecodeString(enc)
		if err != nil {
			return err
		}
		p.Path = append(p.Path, k)
	}
	if dto.Key != "" {
		k, err := base64.StdEncoding.DecodeString(dto.Key)
		if err != nil {
			return err
		}
		p.Key = k
	}
	return nil
}

func loadResumableCompactCheckpoint(path string) (*resumableCompactCheckpoint, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	cp := &resumableCompactCheckpoint{}
	if err := json.Unmarshal(data, cp); err != nil {
		return nil, true, fmt.Errorf("%w: %v", berrors.ErrCompactCheckpointCorrupt, err)
	}
	if cp.Version != resumableCompactCheckpointVersion {
		return nil, true, fmt.Errorf("%w: unsupported version %d", berrors.ErrCompactCheckpointCorrupt, cp.Version)
	}
	switch cp.Status {
	case resumableCompactStatusCopying, resumableCompactStatusStaged:
	default:
		return nil, true, fmt.Errorf("%w: unknown status %q", berrors.ErrCompactCheckpointCorrupt, cp.Status)
	}
	return cp, true, nil
}

// write atomically persists the checkpoint: write and fsync a temp file, rename
// it onto the checkpoint path and fsync the directory. A crash at any point
// leaves either the previous checkpoint or the new one, never a torn file.
func (cp *resumableCompactCheckpoint) write(path, tmpPath string) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
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
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

// copyWithCheckpoints walks the pinned source snapshot and copies every item
// into dst, committing and persisting a checkpoint whenever batchSize source
// bytes have been processed. All write operations are idempotent, so the single
// boundary item replayed on resume cannot cause duplicates or drift.
func copyWithCheckpoints(srcTx *Tx, dst *DB, cp *resumableCompactCheckpoint, batchSize int64,
	sidecarPath, tmpSidecarPath string, logger Logger, interrupt func(string, int) error) (retErr error) {
	walker, err := newResumableWalker(srcTx, cp.Position)
	if err != nil {
		return err
	}

	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()

	var batchBytes int64
	for {
		ev, err := walker.next()
		if err != nil {
			return err
		}
		if ev == nil {
			break
		}

		switch ev.kind {
		case compactEventEnterBucket:
			if err := copyBucketEntry(tx, ev.path, ev.key, ev.seq); err != nil {
				return err
			}
		case compactEventKV:
			if err := copyKeyValue(tx, ev.path, ev.key, ev.value); err != nil {
				return err
			}
			if !ev.replayed {
				cp.Position.Items++
				cp.Position.Bytes += int64(len(ev.key) + len(ev.value))
				batchBytes += int64(len(ev.key) + len(ev.value))
			}
		case compactEventLeaveBucket:
			// A subtree is complete; no write is needed, but the event is a
			// safe checkpoint boundary.
		}

		// Checkpoints may only be taken after key/value items or fully walked
		// buckets, never between entering a bucket and walking its subtree, and
		// never for the single item replayed at a resume boundary.
		if ev.replayed || batchBytes < batchSize ||
			(ev.kind != compactEventKV && ev.kind != compactEventLeaveBucket) {
			continue
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		cp.Position = resumableCompactPosition{
			Path:          cloneByteSlices(ev.path),
			Key:           cloneBytes(ev.key),
			KeyBucketDone: ev.kind == compactEventLeaveBucket,
			Items:         cp.Position.Items,
			Bytes:         cp.Position.Bytes,
		}
		cp.Batches++
		if err := cp.write(sidecarPath, tmpSidecarPath); err != nil {
			return err
		}
		logger.Debugf("compact: checkpoint %d persisted (%d items, %d bytes)", cp.Batches, cp.Position.Items, cp.Position.Bytes)

		if interrupt != nil {
			if err := interrupt("checkpoint", cp.Batches); err != nil {
				return err
			}
		}

		tx, err = dst.Begin(true)
		if err != nil {
			return err
		}
		batchBytes = 0
	}

	return tx.Commit()
}

func copyBucketEntry(tx *Tx, path [][]byte, key []byte, seq uint64) error {
	if len(path) == 0 {
		b, err := tx.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}
		return b.SetSequence(seq)
	}
	b, err := navigateDstBucket(tx, path)
	if err != nil {
		return err
	}
	b.FillPercent = 1.0
	child, err := b.CreateBucketIfNotExists(key)
	if err != nil {
		return err
	}
	return child.SetSequence(seq)
}

func copyKeyValue(tx *Tx, path [][]byte, key, value []byte) error {
	b, err := navigateDstBucket(tx, path)
	if err != nil {
		return err
	}
	b.FillPercent = 1.0
	return b.Put(key, value)
}

func navigateDstBucket(tx *Tx, path [][]byte) (*Bucket, error) {
	b := tx.Bucket(path[0])
	if b == nil {
		return nil, fmt.Errorf("compact: resume bucket %q missing: %w", path[0], berrors.ErrBucketNotFound)
	}
	for _, name := range path[1:] {
		b = b.Bucket(name)
		if b == nil {
			return nil, fmt.Errorf("compact: resume bucket %q missing: %w", name, berrors.ErrBucketNotFound)
		}
	}
	return b, nil
}

// compactEventKind enumerates the DFS events emitted by resumableWalker.
type compactEventKind int

const (
	compactEventEnterBucket compactEventKind = iota
	compactEventKV
	compactEventLeaveBucket
)

type compactEvent struct {
	kind  compactEventKind
	path  [][]byte // bucket names from the root to the bucket owning key
	key   []byte
	value []byte // non-nil only for key/value events
	seq   uint64 // bucket sequence, only for enter events

	// replayed marks the single boundary item reprocessed when resuming from a
	// checkpoint. It is written idempotently but must not count towards the
	// current batch or trigger a checkpoint, otherwise runs could get stuck
	// replaying the same boundary forever when the batch size is very small.
	replayed bool
}

type compactWalkFrame struct {
	bucket *Bucket
	name   []byte   // name of this bucket in its parent; nil for the root frame
	path   [][]byte // names from the root to this bucket; nil for the root frame
	cursor *Cursor

	seek       []byte // when non-nil, the first action is Seek(seek)
	skipSeeked bool   // the seeked item must be skipped (descent key or completed bucket)
	started    bool
}

// resumableWalker is an explicit-stack depth-first walker that can start at an
// arbitrary frontier.
type resumableWalker struct {
	stack         []*compactWalkFrame
	replayPending bool // the next seek-produced item is the resume boundary item
}

// newResumableWalker positions the walker at pos. Ancestor frames are parked on
// the cursor key that leads to the deepest frame; the deepest frame replays the
// single boundary item of pos and continues from there.
func newResumableWalker(tx *Tx, pos resumableCompactPosition) (*resumableWalker, error) {
	w := &resumableWalker{
		// A key/value boundary item is replayed once; a completed-bucket marker
		// is skipped directly.
		replayPending: len(pos.Key) > 0 && !pos.KeyBucketDone,
	}
	rootFrame := &compactWalkFrame{
		bucket: &tx.root,
		cursor: tx.root.Cursor(),
	}
	w.stack = append(w.stack, rootFrame)

	for _, name := range pos.Path {
		parent := w.stack[len(w.stack)-1]
		child := parent.bucket.Bucket(name)
		if child == nil {
			return nil, fmt.Errorf("%w: resume bucket %q missing in source snapshot", berrors.ErrCompactCheckpointCorrupt, name)
		}
		// The descent key belongs to the PARENT bucket's cursor: park the
		// parent frame on it so that it skips the already-processed subtree
		// once the deeper frames finish.
		parent.seek = cloneBytes(name)
		parent.skipSeeked = true
		childPath := make([][]byte, 0, len(parent.path)+1)
		childPath = append(childPath, parent.path...)
		childPath = append(childPath, cloneBytes(name))
		w.stack = append(w.stack, &compactWalkFrame{
			bucket: child,
			name:   cloneBytes(name),
			path:   childPath,
			cursor: child.Cursor(),
		})
	}

	deepest := w.stack[len(w.stack)-1]
	if len(pos.Key) > 0 {
		deepest.seek = cloneBytes(pos.Key)
		deepest.skipSeeked = pos.KeyBucketDone
	}
	return w, nil
}

// next returns the next DFS event, or (nil, nil) when the walk is complete.
func (w *resumableWalker) next() (*compactEvent, error) {
	for len(w.stack) > 0 {
		f := w.stack[len(w.stack)-1]

		var k, v []byte
		var replayed bool
		if !f.started {
			f.started = true
			if f.seek != nil {
				k, v = f.cursor.Seek(f.seek)
				if k == nil {
					return nil, fmt.Errorf("%w: checkpoint key not found in source snapshot", berrors.ErrCompactCheckpointCorrupt)
				}
				if f.skipSeeked {
					k, v = f.cursor.Next()
				} else {
					replayed = w.replayPending
					w.replayPending = false
				}
				f.seek = nil
				f.skipSeeked = false
			} else {
				k, v = f.cursor.First()
			}
		} else {
			k, v = f.cursor.Next()
		}

		if k == nil {
			// Current bucket exhausted: pop it and signal completion to the
			// still-active parent (the root frame completion ends the walk).
			w.stack = w.stack[:len(w.stack)-1]
			if f.name != nil {
				parentPath := f.path[:len(f.path)-1]
				return &compactEvent{
					kind: compactEventLeaveBucket,
					path: parentPath,
					key:  f.name,
				}, nil
			}
			continue
		}

		if child := f.bucket.Bucket(k); child != nil {
			childPath := make([][]byte, 0, len(f.path)+1)
			childPath = append(childPath, f.path...)
			childPath = append(childPath, cloneBytes(k))
			w.stack = append(w.stack, &compactWalkFrame{
				bucket: child,
				name:   cloneBytes(k),
				path:   childPath,
				cursor: child.Cursor(),
			})
			return &compactEvent{
				kind:     compactEventEnterBucket,
				path:     f.path,
				key:      k,
				seq:      child.Sequence(),
				replayed: replayed,
			}, nil
		}

		return &compactEvent{
			kind:     compactEventKV,
			path:     f.path,
			key:      k,
			value:    v,
			replayed: replayed,
		}, nil
	}
	return nil, nil
}

// verifyCompaction streams both databases through the same canonical hash and
// additionally runs bbolt's page-level consistency check on the destination.
// Any discrepancy is reported as an error before the output is published.
func verifyCompaction(srcTx *Tx, dst *DB) (uint64, error) {
	srcHasher := fnv.New64a()
	if err := hashCompactBucket(&srcTx.root, srcHasher); err != nil {
		return 0, fmt.Errorf("compact: failed to hash source database: %w", err)
	}

	var (
		dstHash uint64
	)
	err := dst.View(func(dstTx *Tx) error {
		dstHasher := fnv.New64a()
		if err := hashCompactBucket(&dstTx.root, dstHasher); err != nil {
			return fmt.Errorf("compact: failed to hash compacted database: %w", err)
		}
		dstHash = dstHasher.Sum64()
		return drainCheckChannel(dstTx)
	})
	if err != nil {
		return 0, err
	}
	if srcHasher.Sum64() != dstHash {
		return 0, fmt.Errorf("compact: verification failed: compacted database does not match the source snapshot")
	}
	return dstHash, nil
}

// drainCheckChannel fully consumes the channel returned by Tx.Check and returns
// the first error. The channel must be drained completely so the checker
// goroutine does not block on a full/abandoned channel.
func drainCheckChannel(tx *Tx) error {
	var firstErr error
	for checkErr := range tx.Check() {
		if firstErr == nil {
			firstErr = fmt.Errorf("compact: consistency check of compacted database failed: %w", checkErr)
		}
	}
	return firstErr
}

func hashCompactBucket(b *Bucket, h hash.Hash64) error {
	cursor := b.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if child := b.Bucket(k); child != nil {
			writeCompactHashToken(h, compactHashBucketEnter, k)
			var seqBuf [8]byte
			binary.BigEndian.PutUint64(seqBuf[:], child.Sequence())
			_, _ = h.Write(seqBuf[:])
			if err := hashCompactBucket(child, h); err != nil {
				return err
			}
			_, _ = h.Write([]byte{compactHashBucketLeave})
		} else {
			writeCompactHashToken(h, compactHashKeyValue, k)
			writeCompactHashToken(h, compactHashValue, v)
		}
	}
	return nil
}

const (
	compactHashBucketEnter = 0x01
	compactHashKeyValue    = 0x02
	compactHashBucketLeave = 0x03
	compactHashValue       = 0x04
)

func writeCompactHashToken(h hash.Hash, tag byte, data []byte) {
	_, _ = h.Write([]byte{tag})
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(data)))
	_, _ = h.Write(lenBuf[:n])
	_, _ = h.Write(data)
}

// recoverStagedDestination verifies an already published destination against
// the hash recorded in the staged checkpoint and cleans up the leftover
// artifacts. The source database is intentionally not required here.
func recoverStagedDestination(dstPath, partialPath, sidecarPath, tmpSidecarPath string,
	cp *resumableCompactCheckpoint, logger Logger) error {
	db, err := Open(dstPath, 0, &Options{ReadOnly: true, Logger: logger})
	if err != nil {
		return fmt.Errorf("compact: cannot open published destination for recovery: %w", err)
	}
	var sum uint64
	err = db.View(func(tx *Tx) error {
		h := fnv.New64a()
		if err := hashCompactBucket(&tx.root, h); err != nil {
			return err
		}
		sum = h.Sum64()
		return drainCheckChannel(tx)
	})
	if closeErr := db.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("compact: recovery verification failed: %w", err)
	}
	if cp.ContentHash != fmt.Sprintf("%016x", sum) {
		return fmt.Errorf("%w: published destination content does not match the checkpoint", berrors.ErrCompactCheckpointCorrupt)
	}
	for _, path := range []string{sidecarPath, tmpSidecarPath, partialPath} {
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			return rmErr
		}
	}
	if err := fsyncDir(filepath.Dir(dstPath)); err != nil {
		logger.Warningf("compact: fsync of destination directory failed: %v", err)
	}
	logger.Infof("compact: recovered previously published database at %s", dstPath)
	return nil
}

// verifyFileHash opens path read-only and checks its logical content hash and
// bbolt-level consistency.
func verifyFileHash(path string, logger Logger) (uint64, error) {
	db, err := Open(path, 0, &Options{ReadOnly: true, Logger: logger})
	if err != nil {
		return 0, fmt.Errorf("compact: cannot open %s for recovery verification: %w", path, err)
	}
	var sum uint64
	viewErr := db.View(func(tx *Tx) error {
		h := fnv.New64a()
		if err := hashCompactBucket(&tx.root, h); err != nil {
			return err
		}
		sum = h.Sum64()
		return drainCheckChannel(tx)
	})
	if closeErr := db.Close(); closeErr != nil && viewErr == nil {
		viewErr = closeErr
	}
	return sum, viewErr
}

// finalizeStagedPartial publishes a fully verified partial output without
// consulting the source database: the content hash recorded in the staged
// checkpoint independently proves what the file must contain.
func finalizeStagedPartial(dstPath, partialPath, sidecarPath, tmpSidecarPath string,
	cp *resumableCompactCheckpoint, logger Logger, interrupt func(string, int) error) error {
	sum, err := verifyFileHash(partialPath, logger)
	if err != nil {
		return fmt.Errorf("compact: recovery verification failed: %w", err)
	}
	if cp.ContentHash != fmt.Sprintf("%016x", sum) {
		return fmt.Errorf("%w: partial output content does not match the checkpoint", berrors.ErrCompactCheckpointCorrupt)
	}

	if err := os.Rename(partialPath, dstPath); err != nil {
		return fmt.Errorf("compact: failed to publish destination: %w", err)
	}
	if err := fsyncDir(filepath.Dir(dstPath)); err != nil {
		logger.Warningf("compact: fsync of destination directory failed: %v", err)
	}

	if interrupt != nil {
		if err := interrupt("renamed", cp.Batches); err != nil {
			return err
		}
	}

	for _, path := range []string{sidecarPath, tmpSidecarPath} {
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			return rmErr
		}
	}
	if err := fsyncDir(filepath.Dir(dstPath)); err != nil {
		logger.Warningf("compact: fsync of destination directory failed: %v", err)
	}
	logger.Infof("compact: published compacted database at %s", dstPath)
	return nil
}

func regularFileExists(path string) (bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if fi.IsDir() {
		return false, fmt.Errorf("compact: %s is a directory", path)
	}
	return true, nil
}

func cloneByteSlices(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = cloneBytes(in[i])
	}
	return out
}
