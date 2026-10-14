package bucketinventory_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/bucketinventory"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func openDB(t *testing.T) (*bolt.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func reopenDB(t *testing.T, path string) *bolt.DB {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func manifest(t *testing.T, raw string) *bucketinventory.Manifest {
	t.Helper()
	m, err := bucketinventory.LoadManifest(strings.NewReader(raw))
	require.NoError(t, err)
	return m
}

// dump is a comparable snapshot of a bucket's full content.
type dump struct {
	Values   map[string]string
	Buckets  map[string]*dump
	Sequence uint64
}

func dumpBucket(t *testing.T, b *bolt.Bucket) *dump {
	t.Helper()
	d := &dump{Values: map[string]string{}, Buckets: map[string]*dump{}}
	d.Sequence = b.Sequence()
	err := b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			d.Buckets[string(k)] = dumpBucket(t, child)
			return nil
		}
		d.Values[string(k)] = string(v)
		return nil
	})
	require.NoError(t, err)
	return d
}

func dumpTop(t *testing.T, db *bolt.DB, name string) *dump {
	t.Helper()
	var d *dump
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return errors.New("bucket missing: " + name)
		}
		d = dumpBucket(t, b)
		return nil
	})
	require.NoError(t, err)
	return d
}

func topLevelBuckets(t *testing.T, db *bolt.DB) []string {
	t.Helper()
	var names []string
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if b != nil && string(name) != bucketinventory.MetadataBucket {
				names = append(names, string(name))
			}
			return nil
		})
	})
	require.NoError(t, err)
	sort.Strings(names)
	return names
}

func bucketExists(t *testing.T, db *bolt.DB, name string) bool {
	t.Helper()
	var exists bool
	err := db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket([]byte(name)) != nil
		return nil
	})
	require.NoError(t, err)
	return exists
}

func seedRichBucket(t *testing.T, db *bolt.DB, name string) {
	t.Helper()
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
			return err
		}
		if err := b.Put([]byte("empty-value"), []byte{}); err != nil {
			return err
		}
		// An empty nested bucket must survive renames.
		if _, err := b.CreateBucket([]byte("emptyChild")); err != nil {
			return err
		}
		nested, err := b.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := nested.Put([]byte("a"), []byte("b")); err != nil {
			return err
		}
		inner, err := nested.CreateBucket([]byte("inner"))
		if err != nil {
			return err
		}
		if err := inner.Put([]byte("deep"), []byte("data")); err != nil {
			return err
		}
		if _, err := nested.NextSequence(); err != nil {
			return err
		}
		if _, err := b.NextSequence(); err != nil {
			return err
		}
		if _, err := b.NextSequence(); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
}

func actionTypes(p *bucketinventory.Plan) []bucketinventory.ActionType {
	out := make([]bucketinventory.ActionType, 0, len(p.Actions))
	for _, a := range p.Actions {
		out = append(out, a.Type)
	}
	return out
}

func findAction(p *bucketinventory.Plan, typ bucketinventory.ActionType, id string) (bucketinventory.Action, bool) {
	for _, a := range p.Actions {
		if a.Type == typ && a.ID == id {
			return a, true
		}
	}
	return bucketinventory.Action{}, false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCreateAndConverge(t *testing.T) {
	db, _ := openDB(t)
	r := bucketinventory.New(db)

	m := manifest(t, `{
		"version": 1,
		"buckets": [
			{"id": "svc-a", "bucket": "alpha"},
			{"id": "svc-b", "bucket": "beta"}
		]
	}`)

	// Dry run reports creates and changes nothing.
	plan, err := r.Plan(m)
	require.NoError(t, err)
	assert.Equal(t, []bucketinventory.ActionType{
		bucketinventory.ActionCreate, bucketinventory.ActionCreate,
	}, actionTypes(plan))
	assert.Empty(t, topLevelBuckets(t, db))

	applied, err := r.Apply(m)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, topLevelBuckets(t, db))
	require.Len(t, applied.Actions, 2)

	// Re-running is a no-op: reruns converge.
	again, err := r.Apply(m)
	require.NoError(t, err)
	assert.Equal(t, []bucketinventory.ActionType{
		bucketinventory.ActionUnchanged, bucketinventory.ActionUnchanged,
	}, actionTypes(again))
	assert.Equal(t, []string{"alpha", "beta"}, topLevelBuckets(t, db))
}

func TestRenamePreservesContent(t *testing.T) {
	db, _ := openDB(t)
	seedRichBucket(t, db, "legacy-orders")
	r := bucketinventory.New(db)

	// The pre-existing bucket was created manually: first establish
	// ownership via adopt, then rename via a stable id.
	m1 := manifest(t, `{"buckets":[{"id":"orders","bucket":"legacy-orders","adopt":true}]}`)
	_, err := r.Apply(m1)
	require.NoError(t, err)
	before := dumpTop(t, db, "legacy-orders")

	m2 := manifest(t, `{"buckets":[{"id":"orders","bucket":"orders-v2"}]}`)
	plan, err := r.Plan(m2)
	require.NoError(t, err)
	a, found := findAction(plan, bucketinventory.ActionRename, "orders")
	require.True(t, found)
	assert.Equal(t, "legacy-orders", a.From)
	assert.Equal(t, "orders-v2", a.Name)

	_, err = r.Apply(m2)
	require.NoError(t, err)

	assert.False(t, bucketExists(t, db, "legacy-orders"))
	assert.True(t, bucketExists(t, db, "orders-v2"))
	assert.Equal(t, before, dumpTop(t, db, "orders-v2"))
	assert.Equal(t, uint64(2), dumpTop(t, db, "orders-v2").Sequence)

	// Renaming again converges as well and keeps content.
	m3 := manifest(t, `{"buckets":[{"id":"orders","bucket":"orders-v3"}]}`)
	_, err = r.Apply(m3)
	require.NoError(t, err)
	assert.False(t, bucketExists(t, db, "orders-v2"))
	assert.Equal(t, before, dumpTop(t, db, "orders-v3"))
}

func TestRenameChainAndSwap(t *testing.T) {
	t.Run("chain A->B, B->C with C free", func(t *testing.T) {
		db, _ := openDB(t)
		r := bucketinventory.New(db)
		m0 := manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`)
		_, err := r.Apply(m0)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("A")).Put([]byte("x"), []byte("1"))
		}))
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("B")).Put([]byte("y"), []byte("2"))
		}))

		m1 := manifest(t, `{"buckets":[{"id":"a","bucket":"B"},{"id":"b","bucket":"C"}]}`)
		_, err = r.Apply(m1)
		require.NoError(t, err)

		assert.False(t, bucketExists(t, db, "A"))
		assert.Equal(t, "1", dumpTop(t, db, "B").Values["x"])
		assert.Equal(t, "2", dumpTop(t, db, "C").Values["y"])
	})

	t.Run("swap A<->B two-cycle", func(t *testing.T) {
		db, _ := openDB(t)
		r := bucketinventory.New(db)
		_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`))
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			if err := tx.Bucket([]byte("A")).Put([]byte("who"), []byte("a-data")); err != nil {
				return err
			}
			return tx.Bucket([]byte("B")).Put([]byte("who"), []byte("b-data"))
		}))

		_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"B"},{"id":"b","bucket":"A"}]}`))
		require.NoError(t, err)

		assert.Equal(t, "a-data", dumpTop(t, db, "B").Values["who"])
		assert.Equal(t, "b-data", dumpTop(t, db, "A").Values["who"])
		assert.Equal(t, []string{"A", "B"}, topLevelBuckets(t, db), "no temporary bucket left behind")

		// Re-running the same swap manifest is stable.
		_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"B"},{"id":"b","bucket":"A"}]}`))
		require.NoError(t, err)
		assert.Equal(t, "a-data", dumpTop(t, db, "B").Values["who"])
		assert.Equal(t, "b-data", dumpTop(t, db, "A").Values["who"])
	})

	t.Run("three-cycle A->B->C->A", func(t *testing.T) {
		db, _ := openDB(t)
		r := bucketinventory.New(db)
		_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"},{"id":"c","bucket":"C"}]}`))
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			for kv := range map[string]string{"A": "1", "B": "2", "C": "3"} {
				if err := tx.Bucket([]byte(kv)).Put([]byte("k"), []byte(kv+"-v")); err != nil {
					return err
				}
			}
			return nil
		}))

		_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"B"},{"id":"b","bucket":"C"},{"id":"c","bucket":"A"}]}`))
		require.NoError(t, err)

		assert.Equal(t, "A-v", dumpTop(t, db, "B").Values["k"])
		assert.Equal(t, "B-v", dumpTop(t, db, "C").Values["k"])
		assert.Equal(t, "C-v", dumpTop(t, db, "A").Values["k"])
		assert.Equal(t, []string{"A", "B", "C"}, topLevelBuckets(t, db))
	})
}

func TestReleaseLeavesBucketUntouched(t *testing.T) {
	db, _ := openDB(t)
	r := bucketinventory.New(db)
	m0 := manifest(t, `{"buckets":[{"id":"keep","bucket":"kept"},{"id":"gone","bucket":"dropped"}]}`)
	_, err := r.Apply(m0)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("dropped")).Put([]byte("important"), []byte("data"))
	}))

	// "gone" disappears from the manifest: ownership is released but the
	// bucket and its data must remain.
	m1 := manifest(t, `{"buckets":[{"id":"keep","bucket":"kept"}]}`)
	plan, err := r.Plan(m1)
	require.NoError(t, err)
	rel, found := findAction(plan, bucketinventory.ActionRelease, "gone")
	require.True(t, found)
	assert.Equal(t, "dropped", rel.Name)

	_, err = r.Apply(m1)
	require.NoError(t, err)
	assert.True(t, bucketExists(t, db, "dropped"), "released bucket is never deleted")
	assert.Equal(t, "data", dumpTop(t, db, "dropped").Values["important"])

	st, err := r.Status()
	require.NoError(t, err)
	assert.Equal(t, []string{"dropped"}, st.Unmanaged)
	require.Len(t, st.Managed, 1)
	assert.Equal(t, "keep", st.Managed[0].ID)

	// A released bucket is now ordinary: a new id cannot silently take it
	// over without adopt.
	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"keep","bucket":"kept"},{"id":"reuse","bucket":"dropped"}]}`))
	require.Error(t, err)
	var issues *bucketinventory.IssuesError
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, strings.Join(issues.Issues, "\n"), "not managed")

	// Renaming onto a name that another id releases IN THE SAME BATCH is
	// rejected too: the released data stays and keeps the name occupied.
	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`))
	require.NoError(t, err)
	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"B"}]}`))
	require.Error(t, err)
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, issues.Error(), "released")
	assert.True(t, bucketExists(t, db, "A"), "rejected rename must not move the source")
	assert.Equal(t, "data", dumpTop(t, db, "dropped").Values["important"])
}

func TestManualBucketIsNeverTouched(t *testing.T) {
	db, _ := openDB(t)
	// "manual" exists before the reconciler ever ran.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("manual"))
		if err != nil {
			return err
		}
		return b.Put([]byte("only-humans"), []byte("touch-this"))
	}))
	r := bucketinventory.New(db)

	// A batch that would collide with the manual bucket must fail entirely,
	// even though it also contains an unrelated valid create.
	bad := manifest(t, `{"buckets":[
		{"id":"ok","bucket":"program"},
		{"id":"clash","bucket":"manual"}
	]}`)
	_, err := r.Apply(bad)
	require.Error(t, err)
	var issues *bucketinventory.IssuesError
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, strings.Join(issues.Issues, "\n"), "not managed")

	// Whole batch rolled back: unrelated create and metadata bucket absent.
	assert.Equal(t, []string{"manual"}, topLevelBuckets(t, db))
	assert.False(t, bucketExists(t, db, bucketinventory.MetadataBucket),
		"failed first run must not leave metadata behind")
	assert.Equal(t, "touch-this", dumpTop(t, db, "manual").Values["only-humans"])

	// Same batch with adopt on the manual bucket claims it without changes.
	ok := manifest(t, `{"buckets":[
		{"id":"ok","bucket":"program"},
		{"id":"clash","bucket":"manual","adopt":true}
	]}`)
	_, err = r.Apply(ok)
	require.NoError(t, err)
	assert.Equal(t, "touch-this", dumpTop(t, db, "manual").Values["only-humans"])
	assert.Equal(t, []string{"manual", "program"}, topLevelBuckets(t, db))
}

func TestRenameCollisionsRejected(t *testing.T) {
	db, _ := openDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("human"))
		if err != nil {
			return err
		}
		return b.Put([]byte("keep"), []byte("me"))
	}))
	r := bucketinventory.New(db)
	_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"}]}`))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("A")).Put([]byte("k"), []byte("v"))
	}))

	// Renaming a managed bucket onto a manual bucket is rejected; the whole
	// batch rolls back so the source keeps its content.
	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"human"}]}`))
	require.Error(t, err)
	var issues *bucketinventory.IssuesError
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, issues.Error(), "unmanaged (manual) bucket")
	assert.True(t, bucketExists(t, db, "A"))
	assert.Equal(t, "v", dumpTop(t, db, "A").Values["k"])
	assert.Equal(t, "me", dumpTop(t, db, "human").Values["keep"])
}

func TestCreateIntoNameVacatedByRename(t *testing.T) {
	db, _ := openDB(t)
	r := bucketinventory.New(db)
	_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"}]}`))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("A")).Put([]byte("data"), []byte("old"))
	}))

	// a moves A->C; new id b takes over the now-free name A.
	_, err = r.Apply(manifest(t, `{"buckets":[
		{"id":"a","bucket":"C"},
		{"id":"b","bucket":"A"}
	]}`))
	require.NoError(t, err)
	assert.Equal(t, "old", dumpTop(t, db, "C").Values["data"], "rename carries content")
	assert.Empty(t, dumpTop(t, db, "A").Values, "new bucket at vacated name is empty")
}

func TestAdoptValidation(t *testing.T) {
	db, _ := openDB(t)
	r := bucketinventory.New(db)

	// adopt when nothing exists.
	_, err := r.Apply(manifest(t, `{"buckets":[{"id":"x","bucket":"ghost","adopt":true}]}`))
	require.Error(t, err)
	var issues *bucketinventory.IssuesError
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, issues.Error(), "does not exist")

	// adopt a real unmanaged bucket works.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("real"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}))
	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"x","bucket":"real","adopt":true}]}`))
	require.NoError(t, err)

	// adopt on an already tracked id is rejected.
	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"x","bucket":"real","adopt":true}]}`))
	require.Error(t, err)
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, issues.Error(), "already managed")
}

func TestInvalidManifests(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"unknown version", `{"version":99,"buckets":[]}`, "unsupported manifest version"},
		{"missing id", `{"buckets":[{"bucket":"a"}]}`, "id is required"},
		{"missing name", `{"buckets":[{"id":"1"}]}`, "bucket name is required"},
		{"duplicate id", `{"buckets":[{"id":"x","bucket":"a"},{"id":"x","bucket":"b"}]}`, "duplicate id"},
		{"duplicate name", `{"buckets":[{"id":"1","bucket":"a"},{"id":"2","bucket":"a"}]}`, "duplicate bucket name"},
		{"reserved name", `{"buckets":[{"id":"1","bucket":"__bucket_inventory__"}]}`, "reserved"},
		{"unknown field", `{"buckets":[{"id":"1","bucket":"a","naem":"typo"}]}`, "unknown field"},
		{"trailing data", `{"buckets":[]}{"buckets":[]}`, "trailing data"},
		{"whitespace id", `{"buckets":[{"id":" x","bucket":"a"}]}`, "whitespace"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bucketinventory.LoadManifest(strings.NewReader(tc.raw))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestEmptyManifestReleasesAll(t *testing.T) {
	db, _ := openDB(t)
	r := bucketinventory.New(db)
	_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`))
	require.NoError(t, err)

	plan, err := r.Plan(manifest(t, `{"buckets":[]}`))
	require.NoError(t, err)
	assert.Equal(t, []bucketinventory.ActionType{
		bucketinventory.ActionRelease, bucketinventory.ActionRelease,
	}, actionTypes(plan))

	_, err = r.Apply(manifest(t, `{"buckets":[]}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B"}, topLevelBuckets(t, db), "buckets remain on disk")
	st, err := r.Status()
	require.NoError(t, err)
	assert.Empty(t, st.Managed)
	assert.ElementsMatch(t, []string{"A", "B"}, st.Unmanaged)
}

func TestRerunAfterReopenConverges(t *testing.T) {
	db, path := openDB(t)
	r := bucketinventory.New(db)
	m1 := manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`)
	_, err := r.Apply(m1)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Simulate a restart: reopen the same file and reconcile a new manifest
	// that renames and adds buckets.
	db = reopenDB(t, path)
	r = bucketinventory.New(db)
	m2 := manifest(t, `{"buckets":[{"id":"a","bucket":"A2"},{"id":"b","bucket":"B"},{"id":"c","bucket":"C"}]}`)
	plan, err := r.Plan(m2)
	require.NoError(t, err)
	assert.Equal(t, []bucketinventory.ActionType{
		bucketinventory.ActionRename, bucketinventory.ActionUnchanged, bucketinventory.ActionCreate,
	}, actionTypes(plan))
	_, err = r.Apply(m2)
	require.NoError(t, err)
	assert.Equal(t, []string{"A2", "B", "C"}, topLevelBuckets(t, db))

	// Applying again after another reopen is fully convergent.
	require.NoError(t, db.Close())
	db = reopenDB(t, path)
	r = bucketinventory.New(db)
	again, err := r.Apply(m2)
	require.NoError(t, err)
	for _, a := range again.Actions {
		assert.Equal(t, bucketinventory.ActionUnchanged, a.Type)
	}
}

// TestStateDriftMissingBucket covers the situation left by an old,
// non-atomic tooling: ownership state references a bucket that is gone.
// The reconciler reports the drift and refuses to mutate anything rather
// than silently recreating an empty bucket.
func TestStateDriftMissingBucket(t *testing.T) {
	db, _ := openDB(t)
	r := bucketinventory.New(db)
	_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`))
	require.NoError(t, err)

	// Out-of-band deletion of a tracked bucket (e.g. legacy tooling).
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte("A"))
	}))

	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"},{"id":"b","bucket":"B"}]}`))
	require.Error(t, err)
	var issues *bucketinventory.IssuesError
	require.ErrorAs(t, err, &issues)
	assert.Contains(t, issues.Error(), "missing")

	// The other managed bucket and state are untouched.
	assert.True(t, bucketExists(t, db, "B"))
}

// TestStateDriftGarbageState checks defensive handling of an unreadable
// metadata bucket left by another tool version.
func TestStateDriftGarbageState(t *testing.T) {
	db, _ := openDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		mb, err := tx.CreateBucket([]byte(bucketinventory.MetadataBucket))
		if err != nil {
			return err
		}
		return mb.Put([]byte("state"), []byte("{not-json"))
	}))
	r := bucketinventory.New(db)
	_, err := r.Apply(manifest(t, `{"buckets":[{"id":"a","bucket":"A"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse reconciler state")
}

func TestStatus(t *testing.T) {
	db, _ := openDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("manual-1"))
		return err
	}))
	r := bucketinventory.New(db)

	before, err := r.Status()
	require.NoError(t, err)
	assert.False(t, before.Initialized)
	assert.Equal(t, []string{"manual-1"}, before.Unmanaged)

	_, err = r.Apply(manifest(t, `{"buckets":[{"id":"m","bucket":"managed-1"}]}`))
	require.NoError(t, err)

	after, err := r.Status()
	require.NoError(t, err)
	assert.True(t, after.Initialized)
	assert.Equal(t, []bucketinventory.ManagedBucket{{ID: "m", Bucket: "managed-1"}}, after.Managed)
	assert.Equal(t, []string{"manual-1"}, after.Unmanaged)
}

func TestManifestRoundTrip(t *testing.T) {
	raw := `{
  "version": 1,
  "buckets": [
    {"id": "x", "bucket": "y"},
    {"id": "z", "bucket": "w", "adopt": true}
  ]
}`
	m := manifest(t, raw)
	assert.Equal(t, 1, m.Version)
	require.Len(t, m.Buckets, 2)
	assert.True(t, m.Buckets[1].Adopt)

	out, err := json.Marshal(m)
	require.NoError(t, err)
	m2, err := bucketinventory.LoadManifest(strings.NewReader(string(out)))
	require.NoError(t, err)
	assert.Equal(t, m.Buckets, m2.Buckets)
}

func TestPlanDoesNotMutateDatabase(t *testing.T) {
	db, path := openDB(t)
	// Ensure the file exists with some content.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("manual"))
		return err
	}))
	infoBefore, err := os.Stat(path)
	require.NoError(t, err)

	r := bucketinventory.New(db)
	for range 5 {
		_, err := r.Plan(manifest(t, `{"buckets":[{"id":"a","bucket":"A"}]}`))
		require.NoError(t, err)
	}
	infoAfter, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, infoBefore.Size(), infoAfter.Size(), "read-only planning must not grow the file")
	assert.Equal(t, []string{"manual"}, topLevelBuckets(t, db))
}
