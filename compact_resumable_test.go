package bbolt

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	berrors "go.etcd.io/bbolt/errors"
)

var errSimulatedCrash = errors.New("simulated interruption")

// openTestDB creates a fresh bbolt file in the test's temp directory.
func openTestDB(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// buildSampleSource creates a database with nested buckets, custom sequences,
// empty buckets, zero-length values and regular key/value pairs. It also grows
// the file with large values that are subsequently deleted so that compaction
// reclaims space.
func buildSampleSource(t *testing.T, path string, topBuckets, keysPerBucket int) {
	t.Helper()
	db, err := Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Tx) error {
		for i := 0; i < topBuckets; i++ {
			name := []byte(fmt.Sprintf("bucket-%03d", i))
			b, err := tx.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			if err := b.SetSequence(uint64(i*100 + 7)); err != nil {
				return err
			}
			for j := 0; j < keysPerBucket; j++ {
				v := []byte(fmt.Sprintf("value-%03d-%04d", i, j))
				if err := b.Put([]byte(fmt.Sprintf("key-%04d", j)), v); err != nil {
					return err
				}
			}
			// Nested bucket, including another level deeper.
			sub, err := b.CreateBucketIfNotExists([]byte("sub"))
			if err != nil {
				return err
			}
			if err := sub.SetSequence(42 + uint64(i)); err != nil {
				return err
			}
			if err := sub.Put([]byte("nested-key"), []byte("nested-value")); err != nil {
				return err
			}
			deep, err := sub.CreateBucketIfNotExists([]byte("deep"))
			if err != nil {
				return err
			}
			if err := deep.Put([]byte("deep-key"), []byte("deep-value")); err != nil {
				return err
			}
		}
		// Empty root bucket and an empty nested bucket must round-trip.
		empty, err := tx.CreateBucketIfNotExists([]byte("empty-root"))
		if err != nil {
			return err
		}
		if _, err := empty.CreateBucketIfNotExists([]byte("empty-child")); err != nil {
			return err
		}
		// Zero-length values must be preserved as values.
		zv, err := tx.CreateBucketIfNotExists([]byte("zero-length"))
		if err != nil {
			return err
		}
		return zv.Put([]byte("empty-value"), []byte{})
	}))

	// Fragment the file with large values, then delete them.
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("junk"))
		if err != nil {
			return err
		}
		for i := 0; i < 4; i++ {
			v := make([]byte, 256*1024)
			if _, err := crand.Read(v); err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("junk-%d", i)), v); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("junk"))
	}))
	require.NoError(t, db.Close())
}

func mustCompactHash(t *testing.T, path string) uint64 {
	t.Helper()
	db, err := Open(path, 0, &Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var sum uint64
	require.NoError(t, db.View(func(tx *Tx) error {
		h := fnv.New64a()
		if err := hashCompactBucket(&tx.root, h); err != nil {
			return err
		}
		sum = h.Sum64()
		return nil
	}))
	return sum
}

func countSourceKVs(t *testing.T, path string) int {
	t.Helper()
	db, err := Open(path, 0, &Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	n := 0
	require.NoError(t, walk(db, func(_ [][]byte, _, v []byte, _ uint64) error {
		if v != nil {
			n++
		}
		return nil
	}))
	return n
}

func artifactPaths(dst string) (partial, sidecar, tmp string) {
	return dst + resumableCompactPartialSuffix,
		dst + resumableCompactCheckpointSuffix,
		dst + resumableCompactCheckpointSuffix + resumableCompactSidecarTmpSuffix
}

func TestCompactResumable_FullRun(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 5, 50)

	srcSize := fileSize(t, src)
	require.NoError(t, CompactResumable(src, dst, 0600, nil))

	// Destination is published and the work files are gone.
	assert.True(t, fileExists(dst))
	partial, sidecar, tmp := artifactPaths(dst)
	assert.False(t, fileExists(partial))
	assert.False(t, fileExists(sidecar))
	assert.False(t, fileExists(tmp))

	// Logical content matches the source exactly.
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))

	// Space was reclaimed.
	assert.Less(t, fileSize(t, dst), srcSize)
}

func TestCompactResumable_EmptySource(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	f, err := Open(src, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{BatchSize: 1}))
	assert.True(t, fileExists(dst))
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
}

func TestCompactResumable_ResumeAfterInterruption(t *testing.T) {
	for _, failAtBatch := range []int{1, 2, 5} {
		t.Run(fmt.Sprintf("interrupt-at-batch-%d", failAtBatch), func(t *testing.T) {
			dir := t.TempDir()
			src := filepath.Join(dir, "source.db")
			dst := filepath.Join(dir, "compact.db")
			buildSampleSource(t, src, 6, 40)
			totalKVs := countSourceKVs(t, src)

			partial, sidecar, _ := artifactPaths(dst)

			err := CompactResumable(src, dst, 0600, &ResumableCompactOptions{
				BatchSize: 256,
				interrupt: func(stage string, batches int) error {
					if stage == "checkpoint" && batches == failAtBatch {
						return errSimulatedCrash
					}
					return nil
				},
			})
			require.ErrorIs(t, err, errSimulatedCrash)

			// No half-finished file at the destination path.
			assert.False(t, fileExists(dst))
			assert.True(t, fileExists(partial))
			assert.True(t, fileExists(sidecar))

			// Resume and finish.
			require.NoError(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{BatchSize: 256}))
			assert.True(t, fileExists(dst))
			assert.False(t, fileExists(partial))
			assert.False(t, fileExists(sidecar))
			assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
			assert.Greater(t, totalKVs, 0)
		})
	}
}

func TestCompactResumable_InterruptEveryBatchDeepNested(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")

	// Build a deeply nested chain of buckets with values at every level.
	db, err := Open(src, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("lvl0"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("v0"), []byte("val0")); err != nil {
			return err
		}
		for i := 1; i < 12; i++ {
			b, err = b.CreateBucketIfNotExists([]byte(fmt.Sprintf("lvl%d", i)))
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("v%d", i)), []byte(fmt.Sprintf("val%d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	// Tiny batches: interrupt after every persisted checkpoint until done.
	attempts := 0
	for {
		attempts++
		require.Less(t, attempts, 200, "compaction did not converge")
		err := CompactResumable(src, dst, 0600, &ResumableCompactOptions{
			BatchSize: 1,
			interrupt: func(stage string, _ int) error {
				if stage == "checkpoint" {
					return errSimulatedCrash
				}
				return nil
			},
		})
		if err == nil {
			break
		}
		require.ErrorIs(t, err, errSimulatedCrash)
	}
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
}

func TestCompactResumable_ProgressAdvancesAcrossRuns(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 4, 30)
	_, sidecar, _ := artifactPaths(dst)

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 128,
		interrupt: func(stage string, _ int) error {
			if stage == "checkpoint" {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)

	cp, exists, err := loadResumableCompactCheckpoint(sidecar)
	require.NoError(t, err)
	require.True(t, exists)
	firstItems := cp.Position.Items
	require.Greater(t, firstItems, int64(0))

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 128,
		interrupt: func(stage string, batches int) error {
			if stage == "checkpoint" && batches >= 2 {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)

	cp, _, err = loadResumableCompactCheckpoint(sidecar)
	require.NoError(t, err)
	assert.Greater(t, cp.Position.Items, firstItems, "second run must resume past the first checkpoint")

	require.NoError(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{BatchSize: 128}))
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
}

func TestCompactResumable_SourceChanged(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 4, 20)
	partial, sidecar, _ := artifactPaths(dst)

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 128,
		interrupt: func(stage string, _ int) error {
			if stage == "checkpoint" {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)
	require.True(t, fileExists(partial))

	// Modify the source after the interrupted run.
	modDB, err := Open(src, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, modDB.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("added-after-checkpoint"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}))
	require.NoError(t, modDB.Close())

	err = CompactResumable(src, dst, 0600, &ResumableCompactOptions{BatchSize: 128})
	require.ErrorIs(t, err, berrors.ErrCompactSourceChanged)

	// No misleading output anywhere: destination absent and partial state gone.
	assert.False(t, fileExists(dst))
	assert.False(t, fileExists(partial))
	assert.False(t, fileExists(sidecar))

	// A fresh run compacts the current source successfully.
	require.NoError(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{BatchSize: 128}))
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
}

func TestCompactResumable_CorruptCheckpoint(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 2, 10)
	_, sidecar, _ := artifactPaths(dst)

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 128,
		interrupt: func(stage string, _ int) error {
			if stage == "checkpoint" {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)

	require.NoError(t, os.WriteFile(sidecar, []byte("{not json"), 0600))
	err := CompactResumable(src, dst, 0600, &ResumableCompactOptions{BatchSize: 128})
	require.ErrorIs(t, err, berrors.ErrCompactCheckpointCorrupt)
	assert.False(t, fileExists(dst))
}

func TestCompactResumable_OrphanPartial(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 2, 10)
	partial, _, _ := artifactPaths(dst)

	f, err := os.Create(partial)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = CompactResumable(src, dst, 0600, nil)
	require.ErrorIs(t, err, berrors.ErrCompactPartialExists)
	assert.False(t, fileExists(dst))
}

func TestCompactResumable_OutputExists(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 2, 10)
	f, err := os.Create(dst)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = CompactResumable(src, dst, 0600, nil)
	require.ErrorIs(t, err, berrors.ErrCompactOutputExists)
}

func TestCompactResumable_RestartDiscardsPartial(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 2, 10)
	partial, sidecar, _ := artifactPaths(dst)

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 64,
		interrupt: func(stage string, _ int) error {
			if stage == "checkpoint" {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)
	require.True(t, fileExists(partial))
	require.True(t, fileExists(sidecar))

	require.NoError(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 64,
		Restart:   true,
	}))
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
	assert.False(t, fileExists(partial))
	assert.False(t, fileExists(sidecar))
}

func TestCompactResumable_RecoverStagedBeforePublish(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 3, 15)
	partial, sidecar, _ := artifactPaths(dst)

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 256,
		interrupt: func(stage string, _ int) error {
			if stage == "staged" {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)
	assert.False(t, fileExists(dst))
	assert.True(t, fileExists(partial))
	cp, exists, err := loadResumableCompactCheckpoint(sidecar)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, resumableCompactStatusStaged, cp.Status)
	assert.NotEmpty(t, cp.ContentHash)
	expectedHash := mustCompactHash(t, src)

	// The verified partial output must be publishable even if the source is no
	// longer available: the staged checkpoint carries the content hash.
	movedSrc := src + ".moved"
	require.NoError(t, os.Rename(src, movedSrc))

	// Recovery re-verifies the partial output against the recorded hash and
	// publishes it.
	require.NoError(t, CompactResumable(src, dst, 0600, nil))
	assert.True(t, fileExists(dst))
	assert.False(t, fileExists(partial))
	assert.False(t, fileExists(sidecar))
	assert.Equal(t, expectedHash, mustCompactHash(t, dst))

	require.NoError(t, os.Rename(movedSrc, src))
}

func TestCompactResumable_RecoverStagedAfterPublish(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	dst := filepath.Join(dir, "compact.db")
	buildSampleSource(t, src, 3, 15)
	partial, sidecar, _ := artifactPaths(dst)

	require.ErrorIs(t, CompactResumable(src, dst, 0600, &ResumableCompactOptions{
		BatchSize: 256,
		interrupt: func(stage string, _ int) error {
			if stage == "renamed" {
				return errSimulatedCrash
			}
			return nil
		},
	}), errSimulatedCrash)
	assert.True(t, fileExists(dst))
	assert.True(t, fileExists(sidecar))

	// Recovery must succeed without even needing the source snapshot.
	require.NoError(t, CompactResumable(src, dst, 0600, nil))
	assert.True(t, fileExists(dst))
	assert.False(t, fileExists(partial))
	assert.False(t, fileExists(sidecar))
	assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
}

func TestVerifyCompaction_DetectsDifference(t *testing.T) {
	dir := t.TempDir()
	src := openTestDB(t, filepath.Join(dir, "src.db"))
	dst := openTestDB(t, filepath.Join(dir, "dst.db"))

	require.NoError(t, src.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("same"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k1"), []byte("v1"))
	}))
	require.NoError(t, dst.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("same"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
			return err
		}
		extra, err := tx.CreateBucketIfNotExists([]byte("extra"))
		if err != nil {
			return err
		}
		return extra.Put([]byte("k2"), []byte("v2"))
	}))

	err := src.View(func(srcTx *Tx) error {
		_, err := verifyCompaction(srcTx, dst)
		return err
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verification failed")
}

func TestResumableCompactPositionJSON(t *testing.T) {
	pos := resumableCompactPosition{
		Path:          [][]byte{[]byte("bucket"), []byte("nested")},
		Key:           []byte("last-key"),
		KeyBucketDone: true,
		Items:         11,
		Bytes:         222,
	}
	data, err := pos.MarshalJSON()
	require.NoError(t, err)

	var got resumableCompactPosition
	require.NoError(t, got.UnmarshalJSON(data))
	assert.Equal(t, pos.Path, got.Path)
	assert.Equal(t, pos.Key, got.Key)
	assert.Equal(t, pos.KeyBucketDone, got.KeyBucketDone)
	assert.Equal(t, pos.Items, got.Items)
	assert.Equal(t, pos.Bytes, got.Bytes)

	// The starting position must round-trip too.
	empty := resumableCompactPosition{}
	data, err = empty.MarshalJSON()
	require.NoError(t, err)
	var gotEmpty resumableCompactPosition
	require.NoError(t, gotEmpty.UnmarshalJSON(data))
	assert.Nil(t, gotEmpty.Path)
	assert.Nil(t, gotEmpty.Key)
}

func TestCompactResumable_RandomTreesInterrupted(t *testing.T) {
	// Build random trees with interleaved key/value and bucket siblings, then
	// compact with tiny batches while interrupting at every checkpoint. The
	// resumed result must be logically identical for every shape.
	for seed := int64(0); seed < 8; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := mrand.New(mrand.NewSource(seed))
			dir := t.TempDir()
			src := filepath.Join(dir, "source.db")
			dst := filepath.Join(dir, "compact.db")

			db, err := Open(src, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *Tx) error {
				root, err := tx.CreateBucketIfNotExists([]byte("root"))
				if err != nil {
					return err
				}
				return fillRandomBucket(t, rng, root, 0)
			}))
			require.NoError(t, db.Close())

			batchSize := int64(1 + rng.Intn(50))
			for attempt := 0; ; attempt++ {
				require.Less(t, attempt, 5000)
				err := CompactResumable(src, dst, 0600, &ResumableCompactOptions{
					BatchSize: batchSize,
					interrupt: func(stage string, _ int) error {
						if stage == "checkpoint" {
							return errSimulatedCrash
						}
						return nil
					},
				})
				if err == nil {
					break
				}
				require.ErrorIs(t, err, errSimulatedCrash)
			}
			assert.Equal(t, mustCompactHash(t, src), mustCompactHash(t, dst))
		})
	}
}

// fillRandomBucket populates b with a random mix of values and nested buckets,
// interleaved by key ordering, up to a small depth.
func fillRandomBucket(t *testing.T, rng *mrand.Rand, b *Bucket, depth int) error {
	t.Helper()
	n := 1 + rng.Intn(12)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%03d", rng.Intn(1000)))
		// Skip keys already occupied (by either a value or a bucket).
		if ek, _ := b.Cursor().Seek(key); ek != nil && string(ek) == string(key) {
			continue
		}
		if depth < 3 && rng.Intn(2) == 0 {
			child, err := b.CreateBucket(key)
			if err != nil {
				return err
			}
			if err := child.SetSequence(rng.Uint64() % 100); err != nil {
				return err
			}
			if err := fillRandomBucket(t, rng, child, depth+1); err != nil {
				return err
			}
		} else {
			v := make([]byte, rng.Intn(40))
			if _, err := rng.Read(v); err != nil {
				return err
			}
			if err := b.Put(key, v); err != nil {
				return err
			}
		}
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}
