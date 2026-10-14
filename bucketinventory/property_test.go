package bucketinventory_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/bucketinventory"
)

type walkEntry struct {
	id      string
	name    string
	content string // expected value of key "payload"; "" for freshly created buckets
	adopt   bool   // one-shot: claim an existing unmanaged bucket
}

func buildWalkManifest(entries map[string]*walkEntry) *bucketinventory.Manifest {
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	specs := make([]bucketinventory.BucketSpec, 0, len(ids))
	for _, id := range ids {
		specs = append(specs, bucketinventory.BucketSpec{
			ID:     id,
			Bucket: entries[id].name,
			Adopt:  entries[id].adopt,
		})
	}
	return &bucketinventory.Manifest{Version: 1, Buckets: specs}
}

// TestRandomWalkConvergence drives a long random sequence of reconciliations
// against one database file, reopening it repeatedly as if the process had
// restarted. After every step it asserts the core invariants:
//
//   - state and structure agree: every manifest entry names an existing
//     bucket and every tracked bucket is in exactly the manifest;
//   - bucket content follows the stable id across renames, swaps and
//     releases;
//   - a manually created bucket is never modified;
//   - no temporary rename buckets are ever left behind.
func TestRandomWalkConvergence(t *testing.T) {
	db, path := openDB(t)

	const manual = "human-only"
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(manual))
		if err != nil {
			return err
		}
		return b.Put([]byte("payload"), []byte("manual-content"))
	}))

	rng := rand.New(rand.NewSource(20261014))
	uniq := 0
	freshName := func() string {
		uniq++
		return fmt.Sprintf("bucket-%04d", uniq)
	}

	// entries is the current desired set keyed by stable id.
	entries := map[string]*walkEntry{}
	// untrackedContent records payloads of released/unmanaged buckets by
	// name; the manual bucket lives there forever.
	untrackedContent := map[string]string{manual: "manual-content"}
	idCount := 0

	verify := func() {
		t.Helper()
		var names []string
		err := db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				n := string(name)
				require.NotContains(t, n, "\x00bucket-inventory:tmp:", "temporary bucket leaked")
				if b != nil && n != bucketinventory.MetadataBucket {
					names = append(names, n)
				}
				return nil
			})
		})
		require.NoError(t, err)

		existing := map[string]bool{}
		for _, n := range names {
			existing[n] = true
		}
		managedNames := map[string]bool{}
		for _, e := range entries {
			require.True(t, existing[e.name], "managed bucket %q missing for id %q", e.name, e.id)
			managedNames[e.name] = true
			require.Equal(t, e.content, dumpTop(t, db, e.name).Values["payload"],
				"content mismatch for id %q at bucket %q", e.id, e.name)
		}
		for name, content := range untrackedContent {
			require.True(t, existing[name], "unmanaged bucket %q vanished", name)
			require.False(t, managedNames[name], "bucket %q unexpectedly managed", name)
			require.Equal(t, content, dumpTop(t, db, name).Values["payload"],
				"unmanaged bucket %q content changed", name)
		}

		// Re-planning the same manifest yields only no-ops: reruns converge.
		plan, err := bucketinventory.New(db).Plan(buildWalkManifest(entries))
		require.NoError(t, err)
		for _, a := range plan.Actions {
			require.Equal(t, bucketinventory.ActionUnchanged, a.Type,
				"non-idempotent action on re-run: %+v", a)
		}
	}

	apply := func() {
		t.Helper()
		_, err := bucketinventory.New(db).Apply(buildWalkManifest(entries))
		require.NoError(t, err)
		// Adopt is one-shot and is invalid once the id is tracked.
		for _, e := range entries {
			e.adopt = false
			// Give freshly created empty buckets a payload so content can be
			// tracked across later renames/releases.
			if e.content == "" {
				e.content = fmt.Sprintf("content-of-%s", e.id)
				require.NoError(t, db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte(e.name)).Put([]byte("payload"), []byte(e.content))
				}))
			}
		}
		verify()
	}

	for step := 0; step < 120; step++ {
		ids := make([]string, 0, len(entries))
		for id := range entries {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		switch rng.Intn(5) {
		case 0, 1: // add a new managed bucket (create or adopt a released one)
			idCount++
			id := fmt.Sprintf("id-%03d", idCount)
			name := freshName()
			adopt := false
			for released := range untrackedContent {
				if released != manual && rng.Intn(3) == 0 {
					name = released
					adopt = true
					break
				}
			}
			entries[id] = &walkEntry{id: id, name: name, content: untrackedContent[name], adopt: adopt}
			if adopt {
				delete(untrackedContent, name)
			}
			apply()

		case 2: // rename, sometimes swapping two existing names
			if len(ids) < 2 {
				continue
			}
			i := rng.Intn(len(ids))
			ei := entries[ids[i]]
			if rng.Intn(2) == 0 {
				j := rng.Intn(len(ids))
				for j == i {
					j = rng.Intn(len(ids))
				}
				ej := entries[ids[j]]
				ei.name, ej.name = ej.name, ei.name
			} else {
				ei.name = freshName()
			}
			apply()

		case 3: // release a random managed bucket (its data stays)
			if len(ids) == 0 {
				continue
			}
			id := ids[rng.Intn(len(ids))]
			e := entries[id]
			untrackedContent[e.name] = e.content
			delete(entries, id)
			apply()

		case 4: // plain convergence check, no change
			verify()
		}

		// Periodically reopen the database, simulating a process restart.
		if step%17 == 16 {
			require.NoError(t, db.Close())
			db = reopenDB(t, path)
			verify()
		}
	}

	require.NoError(t, db.Close())
	db = reopenDB(t, path)
	verify()
}
