// Command bucket-inventory reconciles top-level bbolt buckets against an
// external JSON manifest.
//
// Usage:
//
//	bucket-inventory plan   <database> --manifest <file> [--json]
//	bucket-inventory apply  <database> --manifest <file> [--json]
//	bucket-inventory status <database> [--json]
//
// "plan" shows what "apply" would do without touching the database.
// "apply" performs validation, structural changes and the ownership-state
// update in one transaction: on any error (or crash) the whole batch is
// ineffective, and re-running converges to the manifest. "status" lists
// managed and unmanaged (manual) buckets.
//
// Manifest format:
//
//	{
//	  "version": 1,
//	  "buckets": [
//	    {"id": "stable-id", "bucket": "current-bucket-name"},
//	    {"id": "existing-one", "bucket": "manual-bucket", "adopt": true}
//	  ]
//	}
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/bucketinventory"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "plan":
		err = runPlan(args, false)
	case "apply":
		err = runPlan(args, true)
	case "status":
		err = runStatus(args)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		var issues *bucketinventory.IssuesError
		if errors.As(err, &issues) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `bucket-inventory - reconcile bbolt top-level buckets against a manifest

Usage:
  bucket-inventory plan   <database> --manifest <file> [--json]
  bucket-inventory apply  <database> --manifest <file> [--json]
  bucket-inventory status <database> [--json]

Semantics:
  new id + free name      create bucket
  same id, changed name   rename bucket, content preserved
  "adopt": true           claim an existing unmanaged bucket unchanged
  id removed from list    stop managing it (bucket data is kept)
  any other bucket        treated as manual, never modified
`)
}

type commonFlags struct {
	manifest string
	asJSON   bool
}

func parseFlags(args []string, needManifest bool) (*commonFlags, string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cf commonFlags
	fs.StringVar(&cf.manifest, "manifest", "", "path to the bucket inventory manifest (JSON)")
	fs.BoolVar(&cf.asJSON, "json", false, "emit machine-readable JSON")
	// The stdlib flag package stops parsing at the first positional argument,
	// but operators naturally write the database path in the middle; move
	// flags in front of positionals before parsing.
	if err := fs.Parse(reorderFlags(args, "manifest")); err != nil {
		return nil, "", err
	}
	if fs.NArg() != 1 {
		return nil, "", fmt.Errorf("expected exactly one database path")
	}
	if needManifest && cf.manifest == "" {
		return nil, "", fmt.Errorf("--manifest is required")
	}
	return &cf, fs.Arg(0), nil
}

// reorderFlags moves flags (and the values of the named value flags) before
// positional arguments so they parse regardless of argument order.
func reorderFlags(args []string, valueFlags ...string) []string {
	takesValue := map[string]bool{}
	for _, f := range valueFlags {
		takesValue["-"+f] = true
		takesValue["--"+f] = true
	}
	flags := make([]string, 0, len(args))
	positional := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		name := a
		if idx := strings.IndexByte(a, '='); idx >= 0 {
			name = a[:idx]
		}
		if strings.HasPrefix(a, "-") && a != "-" {
			flags = append(flags, a)
			if takesValue[name] && !strings.Contains(a, "=") && i+1 < len(args) {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		positional = append(positional, a)
	}
	return append(flags, positional...)
}

func openDB(path string) (*bolt.DB, func(), error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, nil, err
	}
	return db, func() { _ = db.Close() }, nil
}

func loadManifest(path string) (*bucketinventory.Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return bucketinventory.LoadManifest(f)
}

func runPlan(args []string, execute bool) error {
	cf, dbPath, err := parseFlags(args, true)
	if err != nil {
		return err
	}
	m, err := loadManifest(cf.manifest)
	if err != nil {
		return err
	}
	db, closeDB, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer closeDB()

	r := bucketinventory.New(db)
	var plan *bucketinventory.Plan
	if execute {
		plan, err = r.Apply(m)
	} else {
		plan, err = r.Plan(m)
	}
	if err != nil {
		return err
	}

	if cf.asJSON {
		out := map[string]any{"applied": execute, "plan": plan}
		raw, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(raw))
		return nil
	}

	verb := "Would perform"
	if execute {
		verb = "Applied"
	}
	if len(plan.Actions) == 0 {
		fmt.Println("No changes needed; database already matches the manifest.")
		return nil
	}
	fmt.Printf("%s %d action(s):\n", verb, len(plan.Actions))
	for _, a := range plan.Actions {
		switch a.Type {
		case bucketinventory.ActionCreate:
			fmt.Printf("  - create  bucket %-30q (id %q)\n", a.Name, a.ID)
		case bucketinventory.ActionRename:
			fmt.Printf("  - rename  bucket %-29q -> %q (id %q, content preserved)\n", a.From, a.Name, a.ID)
		case bucketinventory.ActionAdopt:
			fmt.Printf("  - adopt   bucket %-30q (id %q, content untouched)\n", a.Name, a.ID)
		case bucketinventory.ActionRelease:
			fmt.Printf("  - release bucket %-30q (id %q, data kept)\n", a.Name, a.ID)
		case bucketinventory.ActionUnchanged:
			fmt.Printf("  - keep    bucket %-30q (id %q)\n", a.Name, a.ID)
		}
	}
	return nil
}

func runStatus(args []string) error {
	cf, dbPath, err := parseFlags(args, false)
	if err != nil {
		return err
	}
	db, closeDB, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer closeDB()

	st, err := bucketinventory.New(db).Status()
	if err != nil {
		return err
	}
	if cf.asJSON {
		raw, _ := json.MarshalIndent(st, "", "  ")
		fmt.Println(string(raw))
		return nil
	}

	state := "not initialized"
	if st.Initialized {
		state = "initialized"
	}
	fmt.Printf("Reconciler state: %s\n", state)
	if len(st.Managed) > 0 {
		fmt.Printf("Managed buckets (%d):\n", len(st.Managed))
		for _, m := range st.Managed {
			fmt.Printf("  - %-30q id=%q\n", m.Bucket, m.ID)
		}
	} else {
		fmt.Println("Managed buckets: none")
	}
	if len(st.Unmanaged) > 0 {
		fmt.Printf("Unmanaged (manual) buckets (%d):\n", len(st.Unmanaged))
		for _, name := range st.Unmanaged {
			fmt.Printf("  - %q\n", name)
		}
	} else {
		fmt.Println("Unmanaged (manual) buckets: none")
	}
	if len(st.RootKeyValues) > 0 {
		fmt.Printf("Non-bucket root keys (%d):\n", len(st.RootKeyValues))
		for _, name := range st.RootKeyValues {
			fmt.Printf("  - %q\n", name)
		}
	}
	return nil
}
