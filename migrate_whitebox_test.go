package bbolt

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	berrors "go.etcd.io/bbolt/errors"
)

func wbOpen(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), 0600, nil)
	require.NoError(t, err)
	return db
}

// wbBinding builds an internal KeyBinding for whitebox tests.
func wbBinding(key, bucket string) KeyBinding {
	return KeyBinding{Key: []byte(key), Bucket: []byte(bucket)}
}

func wbSeed(t *testing.T, db *DB, pairs map[string]string) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("objects"))
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

// TestMigration_ResumeAfterKillBetweenPrepareAndApply simulates the process
// being killed after the "prepared" manifest records are durable but before
// the apply transaction commits. A fresh process opening the same file must
// converge by finishing the batch.
func TestMigration_ResumeAfterKillBetweenPrepareAndApply(t *testing.T) {
	db := wbOpen(t)
	path := db.Path()
	wbSeed(t, db, map[string]string{"k-a": "v-a", "k-b": "v-b", "k-keep": "v-keep"})

	m := &keyMigrator{
		source:   []byte("objects"),
		manifest: []byte(DefaultMigrationManifestBucket),
		report:   &MigrationReport{BatchID: 1},
	}
	prepared, replayed, _, err := db.stageMigrationBatch(m, &MigrateOptions{BatchID: 1},
		[]KeyBinding{wbBinding("k-a", "tenant-a"), wbBinding("k-b", "tenant-b")})
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, uint64(1), prepared.id)

	// The kill: data must still be entirely in the source bucket.
	require.NoError(t, db.View(func(tx *Tx) error {
		src := tx.Bucket([]byte("objects"))
		require.NotNil(t, src)
		require.Equal(t, []byte("v-a"), src.Get([]byte("k-a")))
		require.Nil(t, tx.Bucket([]byte("tenant-a")))
		return nil
	}))
	require.NoError(t, db.Close())

	// Fresh process: standalone recovery first.
	db2, err := Open(path, 0600, nil)
	require.NoError(t, err)
	defer db2.Close()

	require.NoError(t, db2.RecoverKeyMigrations(nil))

	require.NoError(t, db2.View(func(tx *Tx) error {
		src := tx.Bucket([]byte("objects"))
		require.Equal(t, []byte("v-keep"), src.Get([]byte("k-keep")))
		require.Nil(t, src.Get([]byte("k-a")))
		require.Equal(t, []byte("v-a"), tx.Bucket([]byte("tenant-a")).Get([]byte("k-a")))
		require.Equal(t, []byte("v-b"), tx.Bucket([]byte("tenant-b")).Get([]byte("k-b")))
		return nil
	}))

	// Recovery is idempotent.
	require.NoError(t, db2.RecoverKeyMigrations(nil))
}

// TestMigration_ResumeFinishesPendingBeforeNewBatch ensures a new submission
// first converges an interrupted batch, then applies itself.
func TestMigration_ResumeFinishesPendingBeforeNewBatch(t *testing.T) {
	db := wbOpen(t)
	path := db.Path()
	wbSeed(t, db, map[string]string{"k-a": "v-a", "k-b": "v-b"})

	// Batch 1 is prepared but never applied (killed process).
	m := &keyMigrator{
		source:   []byte("objects"),
		manifest: []byte(DefaultMigrationManifestBucket),
		report:   &MigrationReport{BatchID: 1},
	}
	_, _, _, err := db.stageMigrationBatch(m, &MigrateOptions{BatchID: 1},
		[]KeyBinding{wbBinding("k-a", "tenant-a")})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db2, err := Open(path, 0600, nil)
	require.NoError(t, err)
	defer db2.Close()

	report, err := db2.MigrateKeys([]byte("objects"),
		[]KeyBinding{wbBinding("k-b", "tenant-b")},
		&MigrateOptions{BatchID: 2})
	require.NoError(t, err)
	require.Contains(t, report.RecoveredBatches, uint64(1))
	require.Equal(t, uint64(2), report.BatchID)

	require.NoError(t, db2.View(func(tx *Tx) error {
		require.Equal(t, []byte("v-a"), tx.Bucket([]byte("tenant-a")).Get([]byte("k-a")))
		require.Equal(t, []byte("v-b"), tx.Bucket([]byte("tenant-b")).Get([]byte("k-b")))
		src := tx.Bucket([]byte("objects"))
		require.Nil(t, src.Get([]byte("k-a")))
		require.Nil(t, src.Get([]byte("k-b")))
		return nil
	}))
}

// TestMigration_RecoversMultiplePendingBatchesInOrder ensures batches from
// consecutive killed runs are all finished, in ascending ID order.
func TestMigration_RecoversMultiplePendingBatchesInOrder(t *testing.T) {
	db := wbOpen(t)
	defer db.Close()
	wbSeed(t, db, map[string]string{"k-a": "v-a", "k-b": "v-b"})

	mkStage := func(id uint64, key, dest string) {
		m := &keyMigrator{
			source:   []byte("objects"),
			manifest: []byte(DefaultMigrationManifestBucket),
			report:   &MigrationReport{BatchID: id},
		}
		_, _, _, err := db.stageMigrationBatch(m, &MigrateOptions{BatchID: id},
			[]KeyBinding{wbBinding(key, dest)})
		require.NoError(t, err)
	}
	mkStage(1, "k-a", "tenant-a")
	mkStage(2, "k-b", "tenant-b")

	require.NoError(t, db.RecoverKeyMigrations(nil))
	require.NoError(t, db.View(func(tx *Tx) error {
		require.Equal(t, []byte("v-a"), tx.Bucket([]byte("tenant-a")).Get([]byte("k-a")))
		require.Equal(t, []byte("v-b"), tx.Bucket([]byte("tenant-b")).Get([]byte("k-b")))
		src := tx.Bucket([]byte("objects"))
		require.Nil(t, src.Get([]byte("k-a")))
		require.Nil(t, src.Get([]byte("k-b")))
		return nil
	}))

	// Second recovery run finds no pending work.
	require.NoError(t, db.RecoverKeyMigrations(nil))
}

// TestMigration_ResubmitPendingBatchWithDifferentBindings guards a pending
// batch from being silently replaced by a different plan under the same ID.
func TestMigration_ResubmitPendingBatchWithDifferentBindings(t *testing.T) {
	db := wbOpen(t)
	defer db.Close()
	wbSeed(t, db, map[string]string{"k-a": "v-a"})

	m := &keyMigrator{
		source:   []byte("objects"),
		manifest: []byte(DefaultMigrationManifestBucket),
		report:   &MigrationReport{BatchID: 1},
	}
	_, _, _, err := db.stageMigrationBatch(m, &MigrateOptions{BatchID: 1},
		[]KeyBinding{wbBinding("k-a", "tenant-a")})
	require.NoError(t, err)

	_, _, _, err = db.stageMigrationBatch(m, &MigrateOptions{BatchID: 1},
		[]KeyBinding{wbBinding("k-a", "tenant-b")})
	require.ErrorIs(t, err, berrors.ErrMigrationBatchExists)

	// The original plan is still the one that gets applied.
	require.NoError(t, db.RecoverKeyMigrations(nil))
	require.NoError(t, db.View(func(tx *Tx) error {
		require.Equal(t, []byte("v-a"), tx.Bucket([]byte("tenant-a")).Get([]byte("k-a")))
		require.Nil(t, tx.Bucket([]byte("tenant-b")))
		return nil
	}))
}

// TestMigration_ResumeHealsDuplicateMess covers the real-world aftermath
// described by the task: the old non-atomic mover left the object in both
// buckets. The manifest batch is pending; before it is applied, the duplicate
// state appears. Recovery converges instead of duplicating or erroring.
func TestMigration_ResumeHealsDuplicateMess(t *testing.T) {
	db := wbOpen(t)
	path := db.Path()
	wbSeed(t, db, map[string]string{"k-a": "v-a"})

	m := &keyMigrator{
		source:   []byte("objects"),
		manifest: []byte(DefaultMigrationManifestBucket),
		report:   &MigrationReport{BatchID: 1},
	}
	_, _, _, err := db.stageMigrationBatch(m, &MigrateOptions{BatchID: 1},
		[]KeyBinding{wbBinding("k-a", "tenant-a")})
	require.NoError(t, err)

	// Old mover's debris appears while the batch was pending: same value, two
	// buckets.
	require.NoError(t, db.Update(func(tx *Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("tenant-a"))
		if err != nil {
			return err
		}
		return bk.Put([]byte("k-a"), []byte("v-a"))
	}))
	require.NoError(t, db.Close())

	db2, err := Open(path, 0600, nil)
	require.NoError(t, err)
	defer db2.Close()
	require.NoError(t, db2.RecoverKeyMigrations(nil))

	require.NoError(t, db2.View(func(tx *Tx) error {
		require.Equal(t, []byte("v-a"), tx.Bucket([]byte("tenant-a")).Get([]byte("k-a")))
		require.Nil(t, tx.Bucket([]byte("objects")).Get([]byte("k-a")))
		return nil
	}))
}
