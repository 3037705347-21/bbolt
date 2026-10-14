package bbolt_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

const (
	testSourceBucket   = "objects"
	testManifestBucket = bolt.DefaultMigrationManifestBucket
)

// txRunner is satisfied by both *bolt.DB and the test *btesting.DB wrapper.
type txRunner interface {
	Update(func(*bolt.Tx) error) error
	View(func(*bolt.Tx) error) error
}

// snapshot is bucket -> key -> value for every top-level bucket except the
// migration manifest.
type snapshot map[string]map[string]string

func snapshotDB(t *testing.T, db txRunner) snapshot {
	t.Helper()
	got := snapshot{}
	err := db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for name, v := c.First(); name != nil; name, v = c.Next() {
			if v != nil || string(name) == testManifestBucket {
				continue
			}
			bk := tx.Bucket(name)
			entries := map[string]string{}
			if err := bk.ForEach(func(k, val []byte) error {
				if val == nil {
					return nil // nested bucket, out of scope
				}
				entries[string(k)] = string(val)
				return nil
			}); err != nil {
				return err
			}
			got[string(name)] = entries
		}
		return nil
	})
	require.NoError(t, err)
	return got
}

func seedSourceBucket(t *testing.T, db txRunner, pairs map[string]string) {
	t.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(testSourceBucket))
		if err != nil {
			return err
		}
		for k, v := range pairs {
			if err := bk.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func putKV(t *testing.T, db txRunner, bucket, key, value string) {
	t.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return bk.Put([]byte(key), []byte(value))
	})
	require.NoError(t, err)
}

func deleteKV(t *testing.T, db txRunner, bucket, key string) {
	t.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(bucket))
		if bk == nil {
			return nil
		}
		return bk.Delete([]byte(key))
	})
	require.NoError(t, err)
}

func binding(key, bucket string) bolt.KeyBinding {
	return bolt.KeyBinding{Key: []byte(key), Bucket: []byte(bucket)}
}

func TestDB_MigrateKeys_BasicBatch(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{
		"k-a": "v-a", "k-b": "v-b", "k-keep": "v-keep",
	})

	report, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a"), binding("k-b", "tenant-b")},
		&bolt.MigrateOptions{BatchID: 1},
	)
	require.NoError(t, err)
	require.Equal(t, &bolt.MigrationReport{
		BatchID: 1,
		Moved:   2,
	}, report)

	got := snapshotDB(t, db)
	require.Equal(t, snapshot{
		testSourceBucket: {"k-keep": "v-keep"},
		"tenant-a":       {"k-a": "v-a"},
		"tenant-b":       {"k-b": "v-b"},
	}, got)
}

func TestDB_MigrateKeys_IdempotentReplay(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a"})
	bindings := []bolt.KeyBinding{binding("k-a", "tenant-a")}
	opts := &bolt.MigrateOptions{BatchID: 7}

	report, err := db.MigrateKeys([]byte(testSourceBucket), bindings, opts)
	require.NoError(t, err)
	require.Equal(t, 1, report.Moved)
	want := snapshotDB(t, db)

	// Re-submitting the identical batch is a no-op.
	report2, err := db.MigrateKeys([]byte(testSourceBucket), bindings, opts)
	require.NoError(t, err)
	require.True(t, report2.Replayed)
	require.Zero(t, report2.Moved+report2.Deduplicated+report2.Restored)
	require.Equal(t, want, snapshotDB(t, db))

	// The same ID with different bindings is rejected.
	_, err = db.MigrateKeys([]byte(testSourceBucket), []bolt.KeyBinding{binding("k-a", "tenant-other")}, opts)
	require.ErrorIs(t, err, berrors.ErrMigrationBatchExists)
	require.Equal(t, want, snapshotDB(t, db))
}

func TestDB_MigrateKeys_AutoBatchesConverge(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a", "k-b": "v-b"})
	table := []bolt.KeyBinding{binding("k-a", "tenant-a"), binding("k-b", "tenant-b")}

	r1, err := db.MigrateKeys([]byte(testSourceBucket), table, nil)
	require.NoError(t, err)
	require.Equal(t, 2, r1.Moved)
	require.Equal(t, uint64(1), r1.BatchID)
	want := snapshotDB(t, db)

	// Re-running with a fresh auto batch converges: every key is already in
	// place, and the state stays byte-for-byte identical.
	r2, err := db.MigrateKeys([]byte(testSourceBucket), table, nil)
	require.NoError(t, err)
	require.Equal(t, 2, r2.AlreadyInPlace)
	require.Zero(t, r2.Moved+r2.Deduplicated+r2.Restored)
	require.Equal(t, want, snapshotDB(t, db))

	r3, err := db.MigrateKeys([]byte(testSourceBucket), table, nil)
	require.NoError(t, err)
	require.Equal(t, 2, r3.AlreadyInPlace)
	require.Equal(t, want, snapshotDB(t, db))
}

func TestDB_MigrateKeys_HealsIdenticalDuplicates(t *testing.T) {
	db := btesting.MustCreateDB(t)
	// The old non-atomic mover left k-a in both the source and tenant-a.
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a", "k-b": "v-b"})
	putKV(t, db, "tenant-a", "k-a", "v-a")

	report, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a"), binding("k-b", "tenant-b")},
		&bolt.MigrateOptions{BatchID: 1},
	)
	require.NoError(t, err)
	require.Equal(t, 1, report.Deduplicated)
	require.Equal(t, 1, report.Moved)

	got := snapshotDB(t, db)
	require.Equal(t, "v-a", got["tenant-a"]["k-a"])
	require.NotContains(t, got[testSourceBucket], "k-a")
	require.Equal(t, "v-b", got["tenant-b"]["k-b"])

	// Another run has nothing to deduplicate.
	r2, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a"), binding("k-b", "tenant-b")},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, r2.AlreadyInPlace)
}

func TestDB_MigrateKeys_ConflictAbortsWholeBatch(t *testing.T) {
	db := btesting.MustCreateDB(t)
	// k-a exists in two buckets with disagreeing values.
	seedSourceBucket(t, db, map[string]string{"k-a": "source-value", "k-b": "v-b"})
	putKV(t, db, "tenant-a", "k-a", "tenant value")

	before := snapshotDB(t, db)
	_, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a"), binding("k-b", "tenant-b")},
		&bolt.MigrateOptions{BatchID: 1},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, berrors.ErrMigrationConflict)

	var migrationErrs *bolt.MigrationErrors
	require.ErrorAs(t, err, &migrationErrs)
	require.Len(t, migrationErrs.Conflicts, 1)
	require.Equal(t, "k-a", string(migrationErrs.Conflicts[0].Key))

	// Nothing moved: the batch is all-or-nothing, and no batch got recorded.
	require.Equal(t, before, snapshotDB(t, db))
	require.NoError(t, db.RecoverKeyMigrations(nil))
	require.Equal(t, before, snapshotDB(t, db))
}

func TestDB_MigrateKeys_MissingKeyAborts(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a"})
	before := snapshotDB(t, db)

	_, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("ghost", "tenant-a"), binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1},
	)
	require.ErrorIs(t, err, berrors.ErrMigrationKeyMissing)
	var migrationErrs *bolt.MigrationErrors
	require.ErrorAs(t, err, &migrationErrs)
	require.Len(t, migrationErrs.Missing, 1)
	require.Equal(t, "ghost", string(migrationErrs.Missing[0].Key))

	// k-a must not have moved either.
	require.Equal(t, before, snapshotDB(t, db))
}

func TestDB_MigrateKeys_RestoresFromManifest(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a"})
	_, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1},
	)
	require.NoError(t, err)

	// Later, an external mishap deletes the object from its owning bucket.
	deleteKV(t, db, "tenant-a", "k-a")
	require.NotContains(t, snapshotDB(t, db)["tenant-a"], "k-a")

	// Re-running the table restores it from the staged manifest value.
	report, err := db.MigrateKeys(
		[]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, report.Restored)
	require.Equal(t, "v-a", snapshotDB(t, db)["tenant-a"]["k-a"])
}

func TestDB_MigrateKeys_ReassignAcrossBatches(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a"})

	_, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1})
	require.NoError(t, err)

	// The external table is adjusted: k-a now belongs to tenant-b.
	report, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-b")},
		&bolt.MigrateOptions{BatchID: 2})
	require.NoError(t, err)
	require.Equal(t, 1, report.Moved)

	got := snapshotDB(t, db)
	require.Equal(t, "v-a", got["tenant-b"]["k-a"])
	require.NotContains(t, got["tenant-a"], "k-a")
	require.NotContains(t, got[testSourceBucket], "k-a")
}

func TestDB_MigrateKeys_KeysOutsideTableUntouched(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a", "k-keep": "v-keep"})
	putKV(t, db, "unrelated-bucket", "other-key", "other-value")

	_, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1})
	require.NoError(t, err)

	_, err = db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		nil)
	require.NoError(t, err)

	got := snapshotDB(t, db)
	require.Equal(t, "v-keep", got[testSourceBucket]["k-keep"])
	require.Equal(t, "other-value", got["unrelated-bucket"]["other-key"])
	require.NotContains(t, got[testSourceBucket], "k-a")
}

func TestDB_MigrateKeys_EmptyValue(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-empty": ""})

	report, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-empty", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1})
	require.NoError(t, err)
	require.Equal(t, 1, report.Moved)
	require.Equal(t, "", snapshotDB(t, db)["tenant-a"]["k-empty"])
}

func TestDB_MigrateKeys_EmptyBatchIsNoOp(t *testing.T) {
	db := btesting.MustCreateDB(t)
	seedSourceBucket(t, db, map[string]string{"k-a": "v-a"})
	before := snapshotDB(t, db)

	report, err := db.MigrateKeys([]byte(testSourceBucket), nil, nil)
	require.NoError(t, err)
	require.Equal(t, &bolt.MigrationReport{}, report)
	require.Equal(t, before, snapshotDB(t, db))
}

func TestDB_MigrateKeys_KeysAlreadyInPlaceWithoutSourceBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	// Migration already finished historically; the source bucket is gone.
	putKV(t, db, "tenant-a", "k-a", "v-a")

	report, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1})
	require.NoError(t, err)
	require.Equal(t, 1, report.AlreadyInPlace)
	require.Equal(t, "v-a", snapshotDB(t, db)["tenant-a"]["k-a"])
}

func TestDB_MigrateKeys_NestedBucketIncompatible(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucket([]byte(testSourceBucket))
		if err != nil {
			return err
		}
		if err := bk.Put([]byte("k-a"), []byte("v-a")); err != nil {
			return err
		}
		_, err = bk.CreateBucket([]byte("k-nested"))
		return err
	})
	require.NoError(t, err)
	before := snapshotDB(t, db)

	_, err = db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-nested", "tenant-a"), binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1})
	require.ErrorIs(t, err, berrors.ErrIncompatibleValue)
	require.Equal(t, before, snapshotDB(t, db))
}

func TestDB_MigrateKeys_Validation(t *testing.T) {
	db := btesting.MustCreateDB(t)

	_, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{{Key: nil, Bucket: []byte("tenant-a")}}, nil)
	require.ErrorIs(t, err, berrors.ErrKeyRequired)

	_, err = db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{{Key: []byte("k"), Bucket: nil}}, nil)
	require.ErrorIs(t, err, berrors.ErrBucketNameRequired)

	_, err = db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k", testManifestBucket)}, nil)
	require.ErrorIs(t, err, berrors.ErrMigrationReservedBucket)

	_, err = db.MigrateKeys([]byte(testManifestBucket),
		[]bolt.KeyBinding{binding("k", "tenant-a")}, nil)
	require.ErrorIs(t, err, berrors.ErrMigrationReservedBucket)

	// Same key bound to two buckets in one batch is a conflict.
	_, err = db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k", "tenant-a"), binding("k", "tenant-b")}, nil)
	require.ErrorIs(t, err, berrors.ErrMigrationConflict)
}

func TestRecoverKeyMigrations_NoPending(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.RecoverKeyMigrations(nil))
}

func TestDB_MigrateKeys_ReassignWithStaleDuplicates(t *testing.T) {
	db := btesting.MustCreateDB(t)
	// Messy history: k-a with identical copies in source and the OLD owner;
	// the updated table says tenant-b now owns it.
	putKV(t, db, testSourceBucket, "k-a", "v-a")
	putKV(t, db, "tenant-old", "k-a", "v-a")

	report, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-b")},
		&bolt.MigrateOptions{BatchID: 1})
	require.NoError(t, err)
	require.Equal(t, 1, report.Moved)

	got := snapshotDB(t, db)
	require.Equal(t, "v-a", got["tenant-b"]["k-a"])
	require.NotContains(t, got[testSourceBucket], "k-a")
	require.NotContains(t, got["tenant-old"], "k-a")

	// Convergence: a second auto run is a pure no-op on the object.
	r2, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-b")}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, r2.AlreadyInPlace)
	require.Equal(t, got, snapshotDB(t, db))
}

func TestDB_MigrateKeys_StaleCopyInUnrelatedBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	// k-a is already in tenant-a but a stale copy lingers in another tenant
	// bucket left by an earlier mover run.
	putKV(t, db, "tenant-a", "k-a", "v-a")
	putKV(t, db, "tenant-old", "k-a", "v-a")
	putKV(t, db, "tenant-old", "keep", "keep-v")

	report, err := db.MigrateKeys([]byte(testSourceBucket),
		[]bolt.KeyBinding{binding("k-a", "tenant-a")},
		&bolt.MigrateOptions{BatchID: 1})
	require.NoError(t, err)
	require.Equal(t, 1, report.Deduplicated)

	got := snapshotDB(t, db)
	require.Equal(t, "v-a", got["tenant-a"]["k-a"])
	require.NotContains(t, got["tenant-old"], "k-a")
	// Keys outside the table are never touched.
	require.Equal(t, "keep-v", got["tenant-old"]["keep"])
}
