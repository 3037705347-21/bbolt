package command

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type migrateOptions struct {
	tablePath      string
	batchID        uint64
	recover        bool
	manifestBucket string
}

// migrateTableEntry is one row of the externally maintained ownership table.
type migrateTableEntry struct {
	Key         string `json:"key"`
	Bucket      string `json:"bucket"`
	KeyEncoding string `json:"key_encoding,omitempty"`
}

func newMigrateCommand() *cobra.Command {
	var opts migrateOptions

	cmd := &cobra.Command{
		Use:   "migrate PATH [SOURCE_BUCKET] --table FILE",
		Short: "move keys to their owning buckets according to an ownership table",
		Long: `Move key/value objects out of SOURCE_BUCKET into the top-level buckets named
by an externally maintained ownership table.

The ownership table is a JSON file, either an array of entries:

    [{"key":"object-1","bucket":"tenant-a"},
     {"key":"6f626a6563742d32","bucket":"tenant-b","key_encoding":"hex"}]

or a simple key-to-bucket map:

    {"object-1":"tenant-a","object-2":"tenant-b"}

Key bytes decode as raw UTF-8 by default; key_encoding may be "ascii"
(the default), "hex" or "base64".

The command is safe to run repeatedly: keys not listed in the table are never
touched, each batch either takes effect completely or not at all, and batches
interrupted by a killed process are finished first. Keys already in their
owning bucket converge to no-ops; identical stray copies in other buckets are
removed; keys missing everywhere are restored from the crash-safe manifest.

Use --recover to only finish interrupted batches without submitting a new one.`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			if path == "" {
				return ErrPathRequired
			}
			source := ""
			if len(args) == 2 {
				source = args[1]
			}
			return migrateFunc(cmd, path, source, opts)
		},
	}
	opts.AddFlags(cmd.Flags())

	return cmd
}

func (o *migrateOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.tablePath, "table", "", "path to the ownership table JSON file")
	fs.Uint64Var(&o.batchID, "batch-id", 0, "explicit batch ID for idempotent retries (default: auto-assign)")
	fs.BoolVar(&o.recover, "recover", false, "only finish batches interrupted by a previous run")
	fs.StringVar(&o.manifestBucket, "manifest-bucket", bolt.DefaultMigrationManifestBucket, "top-level bucket holding the migration manifest")
}

func migrateFunc(cmd *cobra.Command, path, source string, opts migrateOptions) error {
	if _, err := checkSourceDBPath(path); err != nil {
		return err
	}

	migrateOpts := &bolt.MigrateOptions{
		BatchID:        opts.batchID,
		ManifestBucket: []byte(opts.manifestBucket),
	}

	if opts.recover {
		// Recovery must be able to run with no SOURCE_BUCKET or table.
		if err := boltOpenMigrate(path, func(db *bolt.DB) error {
			return db.RecoverKeyMigrations(migrateOpts)
		}); err != nil {
			return err
		}
		_, err := fmt.Fprintln(cmd.OutOrStdout(), "migration recovery completed: no interrupted batches pending")
		return err
	}

	if opts.tablePath == "" {
		return fmt.Errorf("--table is required (or use --recover)")
	}
	if source == "" {
		return fmt.Errorf("SOURCE_BUCKET is required when submitting a batch")
	}

	bindings, err := readMigrateTable(opts.tablePath)
	if err != nil {
		return err
	}

	var report *bolt.MigrationReport
	if err := boltOpenMigrate(path, func(db *bolt.DB) error {
		var mErr error
		report, mErr = db.MigrateKeys([]byte(source), bindings, migrateOpts)
		return mErr
	}); err != nil {
		return err
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(),
		"batch %d: moved=%d deduplicated=%d restored=%d already_in_place=%d\n",
		report.BatchID, report.Moved, report.Deduplicated, report.Restored, report.AlreadyInPlace)
	if len(report.RecoveredBatches) > 0 {
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "finished interrupted batches: %v\n", report.RecoveredBatches)
	}
	if report.Replayed {
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "batch %d was already applied: no-op\n", report.BatchID)
	}
	return err
}

func boltOpenMigrate(path string, fn func(*bolt.DB) error) error {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

// readMigrateTable loads the ownership table. Both the array form and the
// key-to-bucket map form are accepted.
func readMigrateTable(path string) ([]bolt.KeyBinding, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var entries []migrateTableEntry
	if err := json.Unmarshal(raw, &entries); err == nil && entries != nil {
		return entriesToBindings(entries)
	}

	var simple map[string]string
	if err := json.Unmarshal(raw, &simple); err != nil {
		return nil, fmt.Errorf("parsing ownership table %q: expected a JSON array of {key,bucket} entries or a key-to-bucket map: %w", path, err)
	}
	entries = make([]migrateTableEntry, 0, len(simple))
	for k, v := range simple {
		entries = append(entries, migrateTableEntry{Key: k, Bucket: v})
	}
	return entriesToBindings(entries)
}

func entriesToBindings(entries []migrateTableEntry) ([]bolt.KeyBinding, error) {
	bindings := make([]bolt.KeyBinding, 0, len(entries))
	for _, e := range entries {
		key, err := decodeMigrateKey(e.Key, e.KeyEncoding)
		if err != nil {
			return nil, fmt.Errorf("invalid key %q: %w", e.Key, err)
		}
		bindings = append(bindings, bolt.KeyBinding{Key: key, Bucket: []byte(e.Bucket)})
	}
	return bindings, nil
}

func decodeMigrateKey(key, encoding string) ([]byte, error) {
	switch encoding {
	case "", "ascii", "utf-8", "utf8":
		return []byte(key), nil
	case "hex":
		return hex.DecodeString(key)
	case "base64":
		return base64.StdEncoding.DecodeString(key)
	default:
		return nil, fmt.Errorf("unsupported key_encoding %q", encoding)
	}
}
