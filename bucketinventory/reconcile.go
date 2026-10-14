package bucketinventory

import (
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// Reconciler plans and applies bucket-inventory reconciliations against one
// open database. A Reconciler is safe for concurrent use because every
// operation runs inside its own bbolt transaction.
type Reconciler struct {
	db *bolt.DB
}

// New returns a Reconciler for the given database.
func New(db *bolt.DB) *Reconciler {
	return &Reconciler{db: db}
}

// Plan computes the changes Apply would make without modifying the
// database. It returns an [*IssuesError] if the manifest or the current
// database state cannot be reconciled safely.
func (r *Reconciler) Plan(m *Manifest) (*Plan, error) {
	if err := ValidateManifest(m); err != nil {
		return nil, err
	}
	var plan *Plan
	err := r.db.View(func(tx *bolt.Tx) error {
		p, err := buildPlan(tx, m)
		if err != nil {
			return err
		}
		plan = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// Apply reconciles the database to the manifest. Validation, structural
// changes and ownership-state update all run in a single write transaction:
// if anything fails (including a manifest/database conflict or a disk write
// error), the transaction is rolled back and the database is left
// untouched. On crash, bbolt's atomic meta-page swap guarantees the
// database reopens in either the pre- or post-Apply state, so re-running
// Apply always converges.
func (r *Reconciler) Apply(m *Manifest) (*Plan, error) {
	if err := ValidateManifest(m); err != nil {
		return nil, err
	}
	var plan *Plan
	err := r.db.Update(func(tx *bolt.Tx) error {
		// Re-plan against the live state inside the write transaction, so
		// the validation and the mutations can never disagree.
		p, err := buildPlan(tx, m)
		if err != nil {
			return err
		}
		if err := p.execute(tx, m); err != nil {
			return fmt.Errorf("apply inventory: %w", err)
		}
		plan = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// Status reports managed and unmanaged top-level buckets.
func (r *Reconciler) Status() (*Status, error) {
	var st Status
	err := r.db.View(func(tx *bolt.Tx) error {
		owned, err := loadState(tx)
		if err != nil {
			return err
		}
		snap, err := snapshotRoot(tx)
		if err != nil {
			return err
		}
		st.Initialized = len(owned) > 0 || tx.Bucket([]byte(MetadataBucket)) != nil
		st.Managed = make([]ManagedBucket, 0, len(owned))
		for id, name := range owned {
			st.Managed = append(st.Managed, ManagedBucket{ID: id, Bucket: name})
		}
		sort.Slice(st.Managed, func(i, j int) bool { return st.Managed[i].ID < st.Managed[j].ID })
		for name := range snap.buckets {
			if _, managed := ownedByBucket(owned, name); managed {
				continue
			}
			st.Unmanaged = append(st.Unmanaged, name)
		}
		sort.Strings(st.Unmanaged)
		st.RootKeyValues = make([]string, 0, len(snap.rawKeys))
		for k := range snap.rawKeys {
			if k == MetadataBucket {
				continue
			}
			st.RootKeyValues = append(st.RootKeyValues, k)
		}
		sort.Strings(st.RootKeyValues)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func ownedByBucket(owned map[string]string, name string) (string, bool) {
	for id, n := range owned {
		if n == name {
			return id, true
		}
	}
	return "", false
}

// renameEdge records one desired id-level rename.
type renameEdge struct {
	id   string
	from string
	to   string
}

func buildPlan(tx *bolt.Tx, m *Manifest) (*Plan, error) {
	owned, err := loadState(tx)
	if err != nil {
		return nil, err
	}
	snap, err := snapshotRoot(tx)
	if err != nil {
		return nil, err
	}

	var issues []string
	if _, collision := snap.rawKeys[MetadataBucket]; collision {
		issues = append(issues, fmt.Sprintf("reserved name %q is occupied by a non-bucket root key; remove or rename it before reconciling", MetadataBucket))
	}

	// Sanity-check state invariants: bucket names must be unique.
	names := make(map[string]string, len(owned))
	for id, name := range owned {
		if other, dup := names[name]; dup {
			issues = append(issues, fmt.Sprintf("corrupt reconciler state: bucket %q claimed by both id %q and id %q", name, other, id))
		}
		names[name] = id
	}

	specByID := make(map[string]BucketSpec, len(m.Buckets))
	for _, s := range m.Buckets {
		specByID[s.ID] = s
	}
	// ownerMovesAway reports whether the given managed id renames to a
	// different bucket name in this batch.
	ownerMovesAway := func(id string) bool {
		s, ok := specByID[id]
		return ok && owned[id] != "" && owned[id] != s.Bucket
	}

	p := &Plan{Actions: make([]Action, 0, len(m.Buckets))}
	var edges []renameEdge

	addIssue := func(format string, args ...any) {
		issues = append(issues, fmt.Sprintf(format, args...))
	}

	for _, s := range m.Buckets {
		curName, tracked := owned[s.ID]
		switch {
		case tracked && curName == s.Bucket:
			if s.Adopt {
				addIssue("id %q: adopt is only valid for new IDs, but bucket %q is already managed by this id", s.ID, s.Bucket)
			}
			if _, ok := snap.buckets[curName]; !ok {
				if _, isRaw := snap.rawKeys[curName]; isRaw {
					addIssue("id %q: tracked bucket %q has been replaced by a non-bucket key; manual intervention required", s.ID, curName)
				} else {
					addIssue("id %q: tracked bucket %q is missing (deleted outside the reconciler?); manual intervention required", s.ID, curName)
				}
			}
			p.Actions = append(p.Actions, Action{Type: ActionUnchanged, ID: s.ID, Name: curName})

		case tracked && curName != s.Bucket:
			if s.Adopt {
				addIssue("id %q: adopt is only valid for new IDs, but the id already tracks bucket %q", s.ID, curName)
			}
			if _, isRaw := snap.rawKeys[curName]; isRaw {
				addIssue("id %q: rename source %q is occupied by a non-bucket key", s.ID, curName)
			} else if _, ok := snap.buckets[curName]; !ok {
				addIssue("id %q: rename source %q is missing (deleted outside the reconciler?); manual intervention required", s.ID, curName)
			}
			checkRenameTarget(s, snap, names, specByID, ownerMovesAway, addIssue)
			edges = append(edges, renameEdge{id: s.ID, from: curName, to: s.Bucket})
			p.Actions = append(p.Actions, Action{Type: ActionRename, ID: s.ID, From: curName, Name: s.Bucket})

		default: // new ID
			switch {
			case isRawKey(snap, s.Bucket):
				addIssue("id %q: desired bucket name %q is occupied by a non-bucket root key", s.ID, s.Bucket)
			case isBucket(snap, s.Bucket):
				if ownerID, isOwned := names[s.Bucket]; isOwned {
					switch {
					case ownerID == s.ID:
						// Unreachable: handled above.
					case ownerMovesAway(ownerID):
						if s.Adopt {
							addIssue("id %q: adopt requested for %q, but that bucket is managed by id %q which renames away in the same batch", s.ID, s.Bucket, ownerID)
						} else {
							// Vacated by the rename: create a fresh bucket
							// afterwards.
							p.creates = append(p.creates, s.Bucket)
							p.Actions = append(p.Actions, Action{Type: ActionCreate, ID: s.ID, Name: s.Bucket})
						}
					default:
						addIssue("id %q: desired bucket name %q is already managed by id %q", s.ID, s.Bucket, ownerID)
					}
				} else {
					// Existing, unmanaged (manual) bucket.
					if s.Adopt {
						p.Actions = append(p.Actions, Action{Type: ActionAdopt, ID: s.ID, Name: s.Bucket})
					} else {
						addIssue("id %q: bucket %q already exists and is not managed; set \"adopt\": true to claim it without modifying its content", s.ID, s.Bucket)
					}
				}
			default:
				if s.Adopt {
					addIssue("id %q: adopt requested but bucket %q does not exist", s.ID, s.Bucket)
				} else {
					p.creates = append(p.creates, s.Bucket)
					p.Actions = append(p.Actions, Action{Type: ActionCreate, ID: s.ID, Name: s.Bucket})
				}
			}
		}
	}

	// Releases: tracked IDs absent from the manifest. Buckets stay in place;
	// only ownership is dropped. Appended after manifest-ordered actions.
	released := make([]stateEntry, 0)
	for id, name := range owned {
		if _, ok := specByID[id]; !ok {
			released = append(released, stateEntry{ID: id, Bucket: name})
		}
	}
	sort.Slice(released, func(i, j int) bool { return released[i].ID < released[j].ID })
	for _, e := range released {
		p.Actions = append(p.Actions, Action{Type: ActionRelease, ID: e.ID, Name: e.Bucket})
	}

	if len(issues) > 0 {
		sort.Strings(issues)
		return nil, &IssuesError{Issues: issues}
	}

	micros, err := orderRenames(edges, snap, m)
	if err != nil {
		return nil, err
	}
	p.renames = micros
	return p, nil
}

// checkRenameTarget validates the destination of a tracked-id rename.
func checkRenameTarget(s BucketSpec, snap *rootSnapshot, owner map[string]string, specByID map[string]BucketSpec, ownerMovesAway func(string) bool, addIssue func(string, ...any)) {
	target := s.Bucket
	switch {
	case isRawKey(snap, target):
		addIssue("id %q: rename target %q is occupied by a non-bucket root key", s.ID, target)
	case isBucket(snap, target):
		ownerID, isOwned := owner[target]
		if !isOwned {
			addIssue("id %q: rename target %q is an unmanaged (manual) bucket; manual buckets are never overwritten", s.ID, target)
			return
		}
		if ownerID == s.ID {
			// Unreachable.
			return
		}
		if !ownerMovesAway(ownerID) {
			if _, inManifest := specByID[ownerID]; inManifest {
				addIssue("id %q: rename target %q stays owned by id %q in this batch", s.ID, target, ownerID)
			} else {
				addIssue("id %q: rename target %q is released (id %q removed from the manifest) but its data is kept and still occupies the name", s.ID, target, ownerID)
			}
			return
		}
		// Otherwise the occupant renames away too: the chain/cycle is
		// resolved with temporary names by orderRenames.
	}
}

func isBucket(snap *rootSnapshot, name string) bool {
	_, ok := snap.buckets[name]
	return ok
}

func isRawKey(snap *rootSnapshot, name string) bool {
	_, ok := snap.rawKeys[name]
	return ok
}

// orderRenames turns id-level renames into an ordered sequence of low-level
// bucket renames that never collides. Chains are applied from the free
// target backwards; cycles are broken with one transient temporary bucket.
func orderRenames(edges []renameEdge, snap *rootSnapshot, m *Manifest) ([]renameMicro, error) {
	if len(edges) == 0 {
		return nil, nil
	}

	// Successor/predecessor maps restricted to names that are themselves
	// renamed away (i.e. occupied by another edge's source).
	succ := make(map[string]string, len(edges))
	pred := make(map[string]string, len(edges))
	for _, e := range edges {
		succ[e.from] = e.to
	}
	for from, to := range succ {
		if _, member := succ[to]; member {
			pred[to] = from
		}
	}

	reserved := func(name string) bool {
		if isBucket(snap, name) || isRawKey(snap, name) {
			return true
		}
		for _, s := range m.Buckets {
			if s.Bucket == name {
				return true
			}
		}
		return false
	}

	var micros []renameMicro
	done := map[string]struct{}{}
	tmpSeq := 0
	newTemp := func() string {
		for {
			tmpSeq++
			candidate := fmt.Sprintf("%s%d", tmpPrefix, tmpSeq)
			if !reserved(candidate) {
				// Treat the new temp as reserved for subsequent picks.
				snap.rawKeys[candidate] = struct{}{}
				return candidate
			}
		}
	}

	// Process chains: start at member names with no predecessor within the
	// rename set and walk to the edge that ends at a free target name.
	heads := make([]string, 0)
	for from := range succ {
		if _, hasPred := pred[from]; !hasPred {
			heads = append(heads, from)
		}
	}
	sort.Strings(heads)
	for _, head := range heads {
		path := []string{head}
		done[head] = struct{}{}
		cur := head
		for {
			next := succ[cur]
			if _, member := succ[next]; !member {
				// Path ends here; next is the free (validated) target.
				micros = append(micros, renameMicro{From: cur, To: next})
				break
			}
			cur = next
			path = append(path, cur)
			done[cur] = struct{}{}
		}
		// Emit the remaining edges backwards so each target is free first.
		for i := len(path) - 2; i >= 0; i-- {
			micros = append(micros, renameMicro{From: path[i], To: path[i+1]})
		}
	}

	// Everything not part of a chain forms pure cycles.
	cycleStarts := make([]string, 0)
	for from := range succ {
		if _, ok := done[from]; !ok {
			cycleStarts = append(cycleStarts, from)
		}
	}
	sort.Strings(cycleStarts)
	for _, start := range cycleStarts {
		if _, ok := done[start]; ok {
			continue
		}
		// Walk the cycle in successor order: n1 -> n2 -> ... -> nk -> n1.
		cycle := []string{start}
		done[start] = struct{}{}
		for cur := succ[start]; cur != start; cur = succ[cur] {
			cycle = append(cycle, cur)
			done[cur] = struct{}{}
		}
		k := len(cycle)
		tmp := newTemp()
		// n1 -> tmp, then nk -> n1, n(k-1) -> nk, ..., n2 -> n3,
		// finally tmp -> n2.
		micros = append(micros, renameMicro{From: cycle[0], To: tmp})
		for i := k - 1; i >= 1; i-- {
			to := cycle[0]
			if i < k-1 {
				to = cycle[i+1]
			}
			micros = append(micros, renameMicro{From: cycle[i], To: to})
		}
		micros = append(micros, renameMicro{From: tmp, To: cycle[1]})
	}

	return micros, nil
}

// execute performs all planned mutations. Called inside the write
// transaction after every check has passed. Structural changes and the
// ownership-state rewrite share this one transaction, so they commit
// together or not at all.
func (p *Plan) execute(tx *bolt.Tx, m *Manifest) error {
	for _, micro := range p.renames {
		if err := renameTopLevelBucket(tx, micro.From, micro.To); err != nil {
			return fmt.Errorf("rename %q to %q: %w", micro.From, micro.To, err)
		}
	}
	for _, name := range p.creates {
		if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
			return fmt.Errorf("create bucket %q: %w", name, err)
		}
	}
	desired := make(map[string]string, len(m.Buckets))
	for _, s := range m.Buckets {
		desired[s.ID] = s.Bucket
	}
	if err := saveState(tx, desired); err != nil {
		return err
	}
	return nil
}

// renameTopLevelBucket renames a top-level bucket while preserving all
// content (nested buckets, key/values and sequence). It is implemented as a
// recursive copy plus removal of the source; because everything runs in the
// caller's transaction, the whole rename is atomic.
func renameTopLevelBucket(tx *bolt.Tx, from, to string) error {
	src := tx.Bucket([]byte(from))
	if src == nil {
		return fmt.Errorf("source bucket %q not found", from)
	}
	dst, err := tx.CreateBucket([]byte(to))
	if err != nil {
		return err
	}
	if err := copyBucket(src, dst); err != nil {
		return err
	}
	if err := dst.SetSequence(src.Sequence()); err != nil {
		return fmt.Errorf("copy sequence: %w", err)
	}
	return tx.DeleteBucket([]byte(from))
}

// copyBucket duplicates all key/values and nested buckets of src into dst.
func copyBucket(src, dst *bolt.Bucket) error {
	return src.ForEach(func(k, v []byte) error {
		if child := src.Bucket(k); child != nil {
			sub, err := dst.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := copyBucket(child, sub); err != nil {
				return err
			}
			return sub.SetSequence(child.Sequence())
		}
		return dst.Put(k, v)
	})
}
