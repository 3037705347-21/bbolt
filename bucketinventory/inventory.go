// Package bucketinventory reconciles the set of top-level bbolt buckets
// against an external manifest.
//
// The reconciler distinguishes buckets it manages from manually created
// buckets by recording ownership in a reserved metadata bucket inside the
// same database. Every managed bucket has a stable ID that survives renames;
// the manifest maps IDs to the desired current bucket name.
//
// Reconciliation is [Reconciler.Apply]-ed inside a single bbolt write
// transaction: the manifest is validated against the live database, all
// structural changes and the ownership state update commit together, or
// nothing commits at all. bbolt swaps its meta page atomically, so a crash
// leaves the database either fully in the old or fully in the new state.
// Re-running Apply with the same manifest therefore always converges:
// already-applied actions become no-ops.
//
// Semantics:
//   - A manifest entry whose ID is new and whose bucket name is free:
//     the bucket is created.
//   - An entry whose ID exists but whose name changed: the bucket is
//     renamed, content (including nested buckets and sequence) preserved.
//   - An entry marked Adopt claims an existing unmanaged bucket without
//     modifying its content, establishing ownership.
//   - An ID missing from the new manifest is released: the bucket itself
//     is left untouched (it is never deleted) and simply stops being
//     tracked.
//   - Any bucket not recorded as managed is treated as manually owned and
//     is never modified. A desired name colliding with such a bucket (or
//     with a non-bucket root key) aborts the whole batch.
package bucketinventory

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// MetadataBucket is the reserved top-level bucket name in which the
// reconciler stores ownership state. It is never considered a managed or
// a manual bucket and must not appear in manifests.
const MetadataBucket = "__bucket_inventory__"

const (
	stateKey            = "state"
	currentStateVersion = 1
	currentManifestV    = 1

	// tmpPrefix names the transient buckets used to break rename cycles.
	// The prefix is chosen so that it cannot be a trimmed manifest name.
	tmpPrefix = "\x00bucket-inventory:tmp:"
)

// Manifest is the external, operator-provided description of the desired
// set of managed buckets.
type Manifest struct {
	// Version is the manifest format version. Zero is accepted and treated
	// as version 1. Unknown future versions are rejected.
	Version int `json:"version"`

	// Buckets is the complete desired set of managed buckets, in
	// deterministic, operator-defined order.
	Buckets []BucketSpec `json:"buckets"`
}

// BucketSpec declares one managed bucket. ID is the stable identity of the
// bucket (it must stay the same across renames); Bucket is the desired
// current top-level bucket name.
type BucketSpec struct {
	// ID uniquely and durably identifies the managed bucket.
	ID string `json:"id"`
	// Bucket is the desired top-level bucket name.
	Bucket string `json:"bucket"`
	// Adopt, when true, claims an already-existing unmanaged bucket under
	// this ID instead of creating an empty one. The bucket content is not
	// modified. It is valid only for previously unmanaged IDs and only
	// when the named bucket already exists.
	Adopt bool `json:"adopt,omitempty"`
}

// Rename describes a planned or executed rename.
type Rename struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

// ManagedBucket pairs a managed bucket's stable ID with its current name.
type ManagedBucket struct {
	ID     string `json:"id"`
	Bucket string `json:"bucket"`
}

// ActionType enumerates the operations a plan can perform.
type ActionType string

// Supported action types.
const (
	// ActionCreate creates a new empty managed bucket.
	ActionCreate ActionType = "create"
	// ActionRename renames a managed bucket, preserving its content.
	ActionRename ActionType = "rename"
	// ActionAdopt records ownership of an existing unmanaged bucket.
	ActionAdopt ActionType = "adopt"
	// ActionRelease drops ownership of a removed bucket without touching it.
	ActionRelease ActionType = "release"
	// ActionUnchanged reports a managed bucket that already matches.
	ActionUnchanged ActionType = "unchanged"
)

// Action is one planned or executed reconciliation step. From is populated
// only for renames; Name holds the (resulting) bucket name otherwise.
type Action struct {
	Type ActionType `json:"type"`
	ID   string     `json:"id"`
	From string     `json:"from,omitempty"`
	Name string     `json:"name,omitempty"`
}

// Plan is the result of reconciling a manifest against the database. It is
// returned both for dry runs ([Reconciler.Plan]) and for executed runs
// ([Reconciler.Apply]); in the latter case it describes what was applied.
type Plan struct {
	// Actions lists all steps in deterministic order: create/rename/adopt/
	// unchanged steps follow manifest order, followed by releases sorted
	// by ID.
	Actions []Action `json:"actions"`

	// Execution instructions, not serialised.
	renames []renameMicro
	creates []string
}

// renameMicro is one ordered low-level rename step. For cycle breaking the
// From name may be a transient temporary bucket.
type renameMicro struct {
	From string
	To   string
}

// Status describes which top-level buckets the reconciler currently knows
// about.
type Status struct {
	// Initialized reports whether ownership state exists in the database.
	Initialized bool `json:"initialized"`
	// Managed lists program-managed buckets sorted by ID.
	Managed []ManagedBucket `json:"managed"`
	// Unmanaged lists existing buckets that are not tracked (manual or
	// released), sorted by name. The metadata bucket is excluded.
	Unmanaged []string `json:"unmanaged"`
	// RootKeyValues lists non-bucket key/value pairs living at the root.
	RootKeyValues []string `json:"rootKeyValues"`
}

// IssuesError reports one or more manifest or database consistency
// problems. When returned from [Reconciler.Apply] no change was committed.
type IssuesError struct {
	Issues []string
}

func (e *IssuesError) Error() string {
	return "reconcile aborted with " + fmt.Sprintf("%d", len(e.Issues)) +
		" issue(s):\n- " + strings.Join(e.Issues, "\n- ")
}

// LoadManifest reads and validates a manifest in JSON format. Unknown JSON
// fields and trailing data are rejected so that manifest typos fail loudly
// instead of silently producing an empty (release-everything) manifest.
func LoadManifest(r io.Reader) (*Manifest, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("parse manifest: unexpected trailing data")
		}
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if err := ValidateManifest(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ValidateManifest performs all checks that do not require database access.
func ValidateManifest(m *Manifest) error {
	var issues []string

	switch m.Version {
	case 0, currentManifestV:
		// 0 is accepted for hand-written manifests and treated as v1.
	default:
		issues = append(issues, fmt.Sprintf("unsupported manifest version %d (supported: %d)", m.Version, currentManifestV))
	}

	seenID := make(map[string]int, len(m.Buckets))
	seenName := make(map[string]int, len(m.Buckets))
	for i, spec := range m.Buckets {
		what := fmt.Sprintf("entry %d", i)
		if spec.ID != "" {
			what = fmt.Sprintf("entry %d (id %q)", i, spec.ID)
		}
		id := strings.TrimSpace(spec.ID)
		name := strings.TrimSpace(spec.Bucket)
		if id == "" {
			issues = append(issues, fmt.Sprintf("%s: id is required", what))
		} else if id != spec.ID {
			issues = append(issues, fmt.Sprintf("%s: id must not have leading or trailing whitespace", what))
		} else if len(id) > bolt.MaxKeySize {
			issues = append(issues, fmt.Sprintf("%s: id too long (%d bytes, max %d)", what, len(id), bolt.MaxKeySize))
		} else if first, dup := seenID[id]; dup {
			issues = append(issues, fmt.Sprintf("duplicate id %q (entries %d and %d)", id, first, i))
		} else {
			seenID[id] = i
		}

		if name == "" {
			issues = append(issues, fmt.Sprintf("%s: bucket name is required", what))
		} else if name != spec.Bucket {
			issues = append(issues, fmt.Sprintf("%s: bucket name must not have leading or trailing whitespace", what))
		} else if len(name) > bolt.MaxKeySize {
			issues = append(issues, fmt.Sprintf("%s: bucket name too long (%d bytes, max %d)", what, len(name), bolt.MaxKeySize))
		} else if name == MetadataBucket {
			issues = append(issues, fmt.Sprintf("%s: bucket name %q is reserved for reconciler metadata", what, name))
		} else if strings.HasPrefix(name, tmpPrefix) {
			issues = append(issues, fmt.Sprintf("%s: bucket name %q uses the reserved temporary-name prefix", what, name))
		} else if first, dup := seenName[name]; dup {
			issues = append(issues, fmt.Sprintf("duplicate bucket name %q (entries %d and %d)", name, first, i))
		} else {
			seenName[name] = i
		}
	}

	if len(issues) > 0 {
		return &IssuesError{Issues: issues}
	}
	return nil
}

// stateEntry is one persisted ownership record.
type stateEntry struct {
	ID     string `json:"id"`
	Bucket string `json:"bucket"`
}

// storedState is the on-disk JSON format of the ownership metadata.
type storedState struct {
	FormatVersion int          `json:"formatVersion"`
	Entries       []stateEntry `json:"entries"`
}

func loadState(tx *bolt.Tx) (map[string]string, error) {
	mb := tx.Bucket([]byte(MetadataBucket))
	if mb == nil {
		return map[string]string{}, nil
	}
	raw := mb.Get([]byte(stateKey))
	if raw == nil {
		return nil, fmt.Errorf("metadata bucket %q exists but contains no reconciler state; refusing to touch a bucket that may be manually owned", MetadataBucket)
	}
	var st storedState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("parse reconciler state: %w", err)
	}
	if st.FormatVersion != currentStateVersion {
		return nil, fmt.Errorf("unsupported reconciler state version %d (supported: %d)", st.FormatVersion, currentStateVersion)
	}
	owned := make(map[string]string, len(st.Entries))
	for _, e := range st.Entries {
		if e.ID == "" || e.Bucket == "" {
			return nil, fmt.Errorf("corrupt reconciler state: empty id or bucket name in entry %+v", e)
		}
		if _, dup := owned[e.ID]; dup {
			return nil, fmt.Errorf("corrupt reconciler state: duplicate id %q", e.ID)
		}
		owned[e.ID] = e.Bucket
	}
	return owned, nil
}

// saveState rewrites the full ownership set inside the same transaction.
func saveState(tx *bolt.Tx, owned map[string]string) error {
	mb, err := tx.CreateBucketIfNotExists([]byte(MetadataBucket))
	if err != nil {
		return fmt.Errorf("create metadata bucket: %w", err)
	}
	entries := make([]stateEntry, 0, len(owned))
	for id, name := range owned {
		entries = append(entries, stateEntry{ID: id, Bucket: name})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	raw, err := json.Marshal(storedState{FormatVersion: currentStateVersion, Entries: entries})
	if err != nil {
		return fmt.Errorf("encode reconciler state: %w", err)
	}
	if err := mb.Put([]byte(stateKey), raw); err != nil {
		return fmt.Errorf("write reconciler state: %w", err)
	}
	return nil
}

// rootSnapshot classifies everything at the database root.
type rootSnapshot struct {
	buckets map[string]struct{}
	rawKeys map[string]struct{}
}

func snapshotRoot(tx *bolt.Tx) (*rootSnapshot, error) {
	snap := &rootSnapshot{
		buckets: map[string]struct{}{},
		rawKeys: map[string]struct{}{},
	}
	err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		if string(name) == MetadataBucket {
			if b != nil {
				// Our own metadata bucket: excluded from both classifications.
				return nil
			}
			// A non-bucket root key occupies the reserved name.
			snap.rawKeys[MetadataBucket] = struct{}{}
			return nil
		}
		if b != nil {
			snap.buckets[string(name)] = struct{}{}
		} else {
			snap.rawKeys[string(name)] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}
