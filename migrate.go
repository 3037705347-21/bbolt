package bbolt

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	berrors "go.etcd.io/bbolt/errors"
)

// DefaultMigrationManifestBucket is the default top-level bucket in which the
// key migration keeps its crash-safe manifest. It can be overridden through
// MigrateOptions.ManifestBucket.
const DefaultMigrationManifestBucket = "_bbolt_key_migration_manifest"

// Migration batch lifecycle states.
const (
	migrationStatePrepared = "prepared"
	migrationStateApplied  = "applied"
)

// Keys used inside the manifest bucket.
var (
	migrationKeySeq     = []byte("seq")
	migrationKeyBatches = []byte("batches")
	migrationKeyCurrent = []byte("current")

	migrationKeySrc   = []byte("source")
	migrationKeyState = []byte("state")
	migrationKeyItems = []byte("items")
)

// KeyBinding binds an object key to its owning bucket. It represents a single
// row of the externally maintained ownership table: the object identified by
// Key must end up in the top-level bucket named Bucket. A binding whose Bucket
// equals the source bucket simply pins the key in place.
type KeyBinding struct {
	// Key is the object key inside the source bucket.
	Key []byte

	// Bucket is the owning (destination) top-level bucket name.
	Bucket []byte
}

// MigrateOptions customizes a key migration run.
type MigrateOptions struct {
	// BatchID identifies the batch in the migration manifest. A value of 0
	// assigns the next sequential ID automatically.
	//
	// Re-submitting the same ID with identical bindings is an idempotent
	// no-op; re-submitting it with different bindings fails with
	// errors.ErrMigrationBatchExists.
	BatchID uint64

	// ManifestBucket is the top-level bucket holding the migration manifest.
	// Defaults to DefaultMigrationManifestBucket when nil.
	ManifestBucket []byte
}

// MigrationLocation describes one live copy of a key.
type MigrationLocation struct {
	// Bucket is the top-level bucket in which the copy was found.
	Bucket string

	// Value is the copy found there.
	Value []byte
}

// MigrationMissingKey describes a key that the ownership table requires but
// which exists in no bucket and for which the manifest holds no staged value.
type MigrationMissingKey struct {
	// Key is the missing object key.
	Key []byte

	// Source is the configured source bucket.
	Source string
}

// MigrationConflictingKey describes a key found in several buckets with
// disagreeing values.
type MigrationConflictingKey struct {
	// Key is the conflicting object key.
	Key []byte

	// Locations are the disagreeing live copies, ordered by bucket name.
	Locations []MigrationLocation
}

// MigrationIncompatibleKey describes a key that is occupied by a nested
// bucket in some bucket and therefore cannot be migrated as a plain
// key/value object.
type MigrationIncompatibleKey struct {
	// Key is the object key.
	Key []byte

	// Bucket is the top-level bucket in which the key is a nested bucket.
	Bucket string
}

// MigrationErrors aggregates every problem found while staging a migration
// batch. A batch is only staged once every binding is problem-free, so any
// non-nil MigrationErrors aborts the whole batch without moving a single key.
type MigrationErrors struct {
	// Missing lists keys that could not be found anywhere.
	Missing []MigrationMissingKey

	// Conflicts lists keys whose live copies disagree.
	Conflicts []MigrationConflictingKey

	// Incompatible lists keys occupied by nested buckets.
	Incompatible []MigrationIncompatibleKey
}

// Is reports whether the aggregate matches errors.ErrMigrationKeyMissing,
// errors.ErrMigrationConflict or errors.ErrIncompatibleValue.
func (e *MigrationErrors) Is(target error) bool {
	switch target {
	case berrors.ErrMigrationKeyMissing:
		return len(e.Missing) > 0
	case berrors.ErrMigrationConflict:
		return len(e.Conflicts) > 0
	case berrors.ErrIncompatibleValue:
		return len(e.Incompatible) > 0
	}
	return false
}

func (e *MigrationErrors) Error() string {
	var b strings.Builder
	switch {
	case len(e.Missing) > 0:
		fmt.Fprintf(&b, "%s: %d key(s) missing from every bucket", berrors.ErrMigrationKeyMissing, len(e.Missing))
	case len(e.Conflicts) > 0:
		fmt.Fprintf(&b, "%s: %d key(s)", berrors.ErrMigrationConflict, len(e.Conflicts))
	case len(e.Incompatible) > 0:
		fmt.Fprintf(&b, "%s: %d key(s) occupied by nested buckets", berrors.ErrIncompatibleValue, len(e.Incompatible))
	}
	details := len(e.Missing) + len(e.Conflicts) + len(e.Incompatible)
	if details > 1 {
		fmt.Fprintf(&b, " (%d problems in total)", details)
	}
	return b.String()
}

// MigrationReport summarizes the outcome of a migration run.
type MigrationReport struct {
	// BatchID is the ID of the batch that was (re)submitted. It is 0 when
	// only crash recovery ran.
	BatchID uint64

	// Replayed is true when the submitted batch ID was already applied and
	// the bindings were identical, so nothing was done.
	Replayed bool

	// RecoveredBatches lists previously prepared, not yet applied batch IDs
	// that were applied during this run's recovery phase.
	RecoveredBatches []uint64

	// Moved counts keys relocated into their owning bucket from elsewhere.
	Moved int

	// AlreadyInPlace counts keys already living in their owning bucket with
	// the staged value and no stale copies elsewhere.
	AlreadyInPlace int

	// Deduplicated counts stale copies removed from non-owning buckets while
	// an identical copy already existed in the owning bucket.
	Deduplicated int

	// Restored counts keys that existed nowhere and were restored from the
	// value staged in the manifest.
	Restored int
}

// stagedRecord is the JSON payload stored for each migrated key in the
// manifest. The value is staged before any data movement so that a crash can
// never turn an object into one that is missing from every bucket.
type stagedRecord struct {
	Bucket string `json:"bucket"`
	Value  []byte `json:"value"`
}

type keyMigrator struct {
	source   []byte
	manifest []byte
	report   *MigrationReport
}

// MigrateKeys moves a batch of key/value objects out of sourceBucket into the
// top-level buckets named by the externally maintained bindings.
//
// The operation is designed to be run repeatedly:
//
//   - The whole batch is staged (object values included) in a crash-safe
//     manifest and only then applied inside a single write transaction, so a
//     batch either takes effect completely or not at all, even if the process
//     is killed mid-run.
//   - Before staging a new batch, batches left in the "prepared" state by a
//     previous interrupted run are applied first (see RecoverKeyMigrations).
//   - Re-running with already applied bindings converges: keys already in
//     their owning bucket are left untouched; identical stray copies left in
//     other buckets (for example by an earlier non-atomic mover) are removed;
//     keys missing everywhere are restored from the staged value.
//   - Keys not present in bindings are never read for writing or moved.
//
// Problems that make the end state ambiguous (missing keys with no staged
// value, or disagreeing copies of the same key) fail the entire batch and are
// returned as *MigrationErrors without changing any data.
func (db *DB) MigrateKeys(sourceBucket []byte, bindings []KeyBinding, opts *MigrateOptions) (*MigrationReport, error) {
	source, manifestName, err := normalizeMigrationOptions(sourceBucket, opts)
	if err != nil {
		return nil, err
	}

	normalized, err := normalizeBindings(bindings, manifestName)
	if err != nil {
		return nil, err
	}

	m := &keyMigrator{
		source:   source,
		manifest: manifestName,
		report:   &MigrationReport{BatchID: optsBatchID(opts)},
	}

	// Phase 0: finish any batch interrupted by a previous process.
	if err := m.recover(db); err != nil {
		return m.report, err
	}

	// Empty batches are valid and simply do nothing (recovery still ran).
	if len(normalized) == 0 {
		return m.report, nil
	}

	// Phase 1: replay check + staging. Nothing user-visible moves here.
	prepared, replayed, wasPending, err := db.stageMigrationBatch(m, opts, normalized)
	if err != nil {
		return m.report, err
	}
	if replayed {
		m.report.Replayed = true
		return m.report, nil
	}
	m.report.BatchID = prepared.id

	// Phase 2: apply the staged batch atomically.
	if err := db.applyMigrationBatch(m, prepared.id, prepared.items); err != nil {
		return m.report, err
	}
	if wasPending {
		// The submitted ID named a batch a killed process had staged but not
		// applied; this run finished it.
		m.report.RecoveredBatches = append(m.report.RecoveredBatches, prepared.id)
	}
	return m.report, nil
}

// RecoverKeyMigrations applies batches found in the manifest in the
// "prepared" but not "applied" state, in ascending batch ID order. It is
// safe (and intended) to call it on every process start. When nothing is
// pending it is a no-op.
func (db *DB) RecoverKeyMigrations(opts *MigrateOptions) error {
	manifestName := effectiveManifestBucket(opts)
	if len(manifestName) == 0 {
		return berrors.ErrBucketNameRequired
	}

	m := &keyMigrator{
		manifest: manifestName,
		report:   &MigrationReport{},
	}
	return m.recover(db)
}

// recover applies every batch left in the "prepared" state, in ascending ID
// order. It is the single convergence entry point shared by new submissions
// and standalone recovery runs.
func (m *keyMigrator) recover(db *DB) error {
	pending, err := db.pendingMigrationBatches(m.manifest)
	if err != nil {
		return err
	}
	for _, p := range pending {
		if err := db.applyMigrationBatch(m, p.id, p.items); err != nil {
			return err
		}
		m.report.RecoveredBatches = append(m.report.RecoveredBatches, p.id)
	}
	return nil
}

func optsBatchID(opts *MigrateOptions) uint64 {
	if opts == nil {
		return 0
	}
	return opts.BatchID
}

func normalizeMigrationOptions(sourceBucket []byte, opts *MigrateOptions) (source []byte, manifest []byte, err error) {
	source = bytes.Clone(sourceBucket)
	manifest = effectiveManifestBucket(opts)
	if len(manifest) == 0 {
		return nil, nil, berrors.ErrBucketNameRequired
	}
	if bytes.Equal(source, manifest) {
		return nil, nil, berrors.ErrMigrationReservedBucket
	}
	return source, manifest, nil
}

func effectiveManifestBucket(opts *MigrateOptions) []byte {
	if opts != nil && len(opts.ManifestBucket) > 0 {
		return bytes.Clone(opts.ManifestBucket)
	}
	return []byte(DefaultMigrationManifestBucket)
}

// normalizeBindings validates a batch and de-duplicates repeated rows. A key
// bound to two different owning buckets within one batch is reported as a
// conflict before the database is touched.
func normalizeBindings(bindings []KeyBinding, manifest []byte) ([]KeyBinding, error) {
	index := make(map[string]int, len(bindings))
	out := make([]KeyBinding, 0, len(bindings))
	var conflict *MigrationErrors

	addConflict := func(k []byte, existing, incoming []byte) {
		if conflict == nil {
			conflict = &MigrationErrors{}
		}
		conflict.Conflicts = append(conflict.Conflicts, MigrationConflictingKey{
			Key: bytes.Clone(k),
			Locations: []MigrationLocation{
				{Bucket: string(existing), Value: nil},
				{Bucket: string(incoming), Value: nil},
			},
		})
	}

	for _, b := range bindings {
		if len(b.Key) == 0 {
			return nil, berrors.ErrKeyRequired
		}
		if len(b.Bucket) == 0 {
			return nil, berrors.ErrBucketNameRequired
		}
		if bytes.Equal(b.Bucket, manifest) {
			return nil, berrors.ErrMigrationReservedBucket
		}
		if pos, ok := index[string(b.Key)]; ok {
			if !bytes.Equal(out[pos].Bucket, b.Bucket) {
				addConflict(b.Key, out[pos].Bucket, b.Bucket)
			}
			continue
		}
		index[string(b.Key)] = len(out)
		out = append(out, KeyBinding{Key: bytes.Clone(b.Key), Bucket: bytes.Clone(b.Bucket)})
	}
	if conflict != nil {
		return nil, conflict
	}
	return out, nil
}

type stagedBatch struct {
	id     uint64
	source []byte
	items  map[string]stagedRecord
}

type pendingBatch struct {
	id     uint64
	source []byte
	items  map[string]stagedRecord
}

// stageMigrationBatch persists the batch plan and the staged values in the
// manifest ("prepared" state) without modifying any user bucket. The
// wasPending result is true when the submitted batch ID named a batch a
// previous process had staged but not applied.
func (db *DB) stageMigrationBatch(m *keyMigrator, opts *MigrateOptions, bindings []KeyBinding) (prepared *stagedBatch, replayed bool, wasPending bool, err error) {
	err = db.Update(func(tx *Tx) error {
		manifest, err := tx.CreateBucketIfNotExists(m.manifest)
		if err != nil {
			return err
		}
		batches, err := manifest.CreateBucketIfNotExists(migrationKeyBatches)
		if err != nil {
			return err
		}
		if _, err := manifest.CreateBucketIfNotExists(migrationKeyCurrent); err != nil {
			return err
		}

		seq := readManifestSeq(manifest)

		// Re-submitting an existing batch ID.
		if opts != nil && opts.BatchID != 0 && opts.BatchID <= seq {
			batchBucket := batches.Bucket(encodeUint64(opts.BatchID))
			if batchBucket == nil {
				return fmt.Errorf("migration: batch %d is missing from the manifest", opts.BatchID)
			}
			state := string(batchBucket.Get(migrationKeyState))
			if state != migrationStateApplied {
				// Prepared but not applied: the recorded plan is the
				// authority; a re-submission must match it.
				items, err := readStagedItems(batchBucket)
				if err != nil {
					return err
				}
				if !sameBindings(items, bindings) {
					return fmt.Errorf("%w: batch %d", berrors.ErrMigrationBatchExists, opts.BatchID)
				}
				prepared = &stagedBatch{
					id:     opts.BatchID,
					source: bytes.Clone(batchBucket.Get(migrationKeySrc)),
					items:  items,
				}
				wasPending = true
				return nil
			}
			items, err := readStagedItems(batchBucket)
			if err != nil {
				return err
			}
			if !sameBindings(items, bindings) {
				return fmt.Errorf("%w: batch %d", berrors.ErrMigrationBatchExists, opts.BatchID)
			}
			replayed = true
			return nil
		}

		id := uint64(0)
		if opts != nil && opts.BatchID != 0 {
			id = opts.BatchID
		} else {
			id = seq + 1
		}

		// Scan every binding against the live database before writing the
		// batch, so an aborted stage leaves no trace.
		currentBucket := manifest.Bucket(migrationKeyCurrent)
		items := make(map[string]stagedRecord, len(bindings))
		scanErrs := &MigrationErrors{}
		for _, b := range bindings {
			record, locs, perr := stageOneKey(tx, m, currentBucket, b)
			if perr != nil {
				switch {
				case errors.Is(perr, berrors.ErrMigrationKeyMissing):
					scanErrs.Missing = append(scanErrs.Missing, MigrationMissingKey{
						Key:    bytes.Clone(b.Key),
						Source: string(m.source),
					})
				case errors.Is(perr, berrors.ErrMigrationConflict):
					scanErrs.Conflicts = append(scanErrs.Conflicts, MigrationConflictingKey{
						Key:       bytes.Clone(b.Key),
						Locations: locs,
					})
				default:
					return perr
				}
				continue
			}
			items[string(b.Key)] = record
		}
		if len(scanErrs.Missing)+len(scanErrs.Conflicts)+len(scanErrs.Incompatible) > 0 {
			return scanErrs
		}

		batchBucket, err := batches.CreateBucket(encodeUint64(id))
		if err != nil {
			return err
		}
		if err := batchBucket.Put(migrationKeySrc, m.source); err != nil {
			return err
		}
		if err := batchBucket.Put(migrationKeyState, []byte(migrationStatePrepared)); err != nil {
			return err
		}
		itemBucket, err := batchBucket.CreateBucket(migrationKeyItems)
		if err != nil {
			return err
		}
		for key, record := range items {
			raw, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := itemBucket.Put([]byte(key), raw); err != nil {
				return err
			}
		}
		if id > seq {
			if err := manifest.Put(migrationKeySeq, encodeUint64(id)); err != nil {
				return err
			}
		}

		prepared = &stagedBatch{id: id, source: bytes.Clone(m.source), items: items}
		return nil
	})
	if err != nil {
		return nil, false, false, err
	}
	return prepared, replayed, wasPending, nil
}

// stageOneKey inspects every top-level bucket for live copies of the bound
// key and decides the staged value:
//
//   - several identical copies: stage the shared value (apply dedupes);
//   - several disagreeing copies: conflict;
//   - one copy: stage its value;
//   - no copy but a staged value in the manifest: restore from the manifest;
//   - no copy and no staged value: unrecoverable missing key.
func stageOneKey(tx *Tx, m *keyMigrator, currentBucket *Bucket, b KeyBinding) (stagedRecord, []MigrationLocation, error) {
	locations, err := findKeyLocations(tx, m.manifest, b.Key)
	if err != nil {
		return stagedRecord{}, nil, err
	}

	if len(locations) >= 2 {
		reference := locations[0].Value
		for _, loc := range locations[1:] {
			if !bytes.Equal(reference, loc.Value) {
				return stagedRecord{}, locations, berrors.ErrMigrationConflict
			}
		}
	}

	if len(locations) > 0 {
		return stagedRecord{Bucket: string(b.Bucket), Value: bytes.Clone(locations[0].Value)}, locations, nil
	}

	if currentBucket != nil {
		if raw := currentBucket.Get(b.Key); raw != nil {
			var previous stagedRecord
			if err := json.Unmarshal(raw, &previous); err != nil {
				return stagedRecord{}, nil, fmt.Errorf("migration: corrupt manifest record for key %q: %w", b.Key, err)
			}
			return stagedRecord{Bucket: string(b.Bucket), Value: bytes.Clone(previous.Value)}, nil, nil
		}
	}

	return stagedRecord{}, nil, berrors.ErrMigrationKeyMissing
}

// findKeyLocations returns every live plain-value copy of key among the
// top-level buckets (the manifest bucket excluded). A key occupied by a
// nested bucket is reported as incompatible.
func findKeyLocations(tx *Tx, manifest, key []byte) ([]MigrationLocation, error) {
	names, err := topLevelBucketNames(tx, manifest)
	if err != nil {
		return nil, err
	}
	var locations []MigrationLocation
	for _, name := range names {
		bk := tx.Bucket(name)
		if bk == nil {
			continue
		}
		if child := bk.Bucket(key); child != nil {
			return nil, &MigrationErrors{Incompatible: []MigrationIncompatibleKey{{
				Key:    bytes.Clone(key),
				Bucket: string(name),
			}}}
		}
		if v := bk.Get(key); v != nil {
			locations = append(locations, MigrationLocation{
				Bucket: string(name),
				Value:  bytes.Clone(v),
			})
		}
	}
	sort.Slice(locations, func(i, j int) bool { return locations[i].Bucket < locations[j].Bucket })
	return locations, nil
}

// topLevelBucketNames enumerates top-level bucket names, skipping the
// manifest bucket. Only bucket entries are returned; plain values stored at
// the root are outside the migration scope.
func topLevelBucketNames(tx *Tx, manifest []byte) ([][]byte, error) {
	var names [][]byte
	c := tx.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v != nil {
			continue // plain root value, not a bucket
		}
		if bytes.Equal(k, manifest) {
			continue
		}
		names = append(names, bytes.Clone(k))
	}
	sort.Slice(names, func(i, j int) bool { return bytes.Compare(names[i], names[j]) < 0 })
	return names, nil
}

// applyMigrationBatch performs the staged moves in a single write
// transaction: each key ends up exactly once, in its owning bucket, with the
// staged value; the batch state flips to "applied" in the same commit, so the
// data movement and the progress marker can never diverge.
func (db *DB) applyMigrationBatch(m *keyMigrator, id uint64, items map[string]stagedRecord) error {
	return db.Update(func(tx *Tx) error {
		manifest, err := tx.CreateBucketIfNotExists(m.manifest)
		if err != nil {
			return err
		}
		batches := manifest.Bucket(migrationKeyBatches)
		if batches == nil {
			return fmt.Errorf("migration: manifest holds no batches")
		}
		batchBucket := batches.Bucket(encodeUint64(id))
		if batchBucket == nil {
			return fmt.Errorf("migration: batch %d is missing from the manifest", id)
		}
		if state := string(batchBucket.Get(migrationKeyState)); state == migrationStateApplied {
			return nil // another process applied it while we waited for the lock
		}

		currentBucket, err := manifest.CreateBucketIfNotExists(migrationKeyCurrent)
		if err != nil {
			return err
		}

		names, err := topLevelBucketNames(tx, m.manifest)
		if err != nil {
			return err
		}

		keys := make([]string, 0, len(items))
		for k := range items {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, keyStr := range keys {
			key := []byte(keyStr)
			record := items[keyStr]
			destName := []byte(record.Bucket)

			dest, err := tx.CreateBucketIfNotExists(destName)
			if err != nil {
				return err
			}

			before := dest.Get(key)
			destHadValue := before != nil
			destValueMatches := destHadValue && bytes.Equal(before, record.Value)
			if child := dest.Bucket(key); child != nil {
				return &MigrationErrors{Incompatible: []MigrationIncompatibleKey{{
					Key:    bytes.Clone(key),
					Bucket: record.Bucket,
				}}}
			}

			copiesElsewhere := 0
			for _, name := range names {
				if bytes.Equal(name, destName) {
					continue
				}
				other := tx.Bucket(name)
				if other == nil {
					continue
				}
				if child := other.Bucket(key); child != nil {
					return &MigrationErrors{Incompatible: []MigrationIncompatibleKey{{
						Key:    bytes.Clone(key),
						Bucket: string(name),
					}}}
				}
				if v := other.Get(key); v != nil {
					if err := other.Delete(key); err != nil {
						return err
					}
					copiesElsewhere++
				}
			}

			if err := dest.Put(key, record.Value); err != nil {
				return err
			}

			raw, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := currentBucket.Put(key, raw); err != nil {
				return err
			}

			switch {
			case !destHadValue && copiesElsewhere == 0:
				m.report.Restored++
			case !destHadValue:
				m.report.Moved++
			case destValueMatches && copiesElsewhere > 0:
				m.report.Deduplicated++
			case destValueMatches:
				m.report.AlreadyInPlace++
			default:
				// The owning bucket held a different value than the staged
				// one; the manifest is authoritative and reinstalls it.
				m.report.Moved++
			}
		}

		if err := batchBucket.Put(migrationKeyState, []byte(migrationStateApplied)); err != nil {
			return err
		}
		return nil
	})
}

// pendingMigrationBatches reads every batch still in the "prepared" state,
// in ascending ID order.
func (db *DB) pendingMigrationBatches(manifestName []byte) ([]pendingBatch, error) {
	var pending []pendingBatch
	err := db.View(func(tx *Tx) error {
		manifest := tx.Bucket(manifestName)
		if manifest == nil {
			return nil
		}
		batches := manifest.Bucket(migrationKeyBatches)
		if batches == nil {
			return nil
		}
		c := batches.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if len(k) != 8 {
				continue // ignore foreign keys
			}
			batchBucket := batches.Bucket(k)
			if batchBucket == nil {
				continue
			}
			if !bytes.Equal(batchBucket.Get(migrationKeyState), []byte(migrationStatePrepared)) {
				continue
			}
			items, err := readStagedItems(batchBucket)
			if err != nil {
				return err
			}
			pending = append(pending, pendingBatch{
				id:     binary.BigEndian.Uint64(k),
				source: bytes.Clone(batchBucket.Get(migrationKeySrc)),
				items:  items,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pending, nil
}

func readStagedItems(batchBucket *Bucket) (map[string]stagedRecord, error) {
	itemBucket := batchBucket.Bucket(migrationKeyItems)
	if itemBucket == nil {
		return nil, fmt.Errorf("migration: batch manifest is missing its items bucket")
	}
	items := make(map[string]stagedRecord)
	err := itemBucket.ForEach(func(k, v []byte) error {
		var record stagedRecord
		if err := json.Unmarshal(v, &record); err != nil {
			return fmt.Errorf("migration: corrupt manifest record for key %q: %w", k, err)
		}
		items[string(k)] = record
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func readManifestSeq(manifest *Bucket) uint64 {
	raw := manifest.Get(migrationKeySeq)
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// sameBindings checks that a re-submitted batch is identical to the recorded
// one.
func sameBindings(items map[string]stagedRecord, bindings []KeyBinding) bool {
	if len(items) != len(bindings) {
		return false
	}
	for _, b := range bindings {
		record, ok := items[string(b.Key)]
		if !ok || record.Bucket != string(b.Bucket) {
			return false
		}
	}
	return true
}
