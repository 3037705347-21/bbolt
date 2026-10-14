package command

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/surgeon"
)

func newSurgeryFreelistCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "freelist <subcommand>",
		Short: "freelist related surgery commands",
	}

	cmd.AddCommand(newSurgeryFreelistAbandonCommand())
	cmd.AddCommand(newSurgeryFreelistRebuildCommand())
	cmd.AddCommand(newSurgeryFreelistRebuildBatchedCommand())

	return cmd
}

func newSurgeryFreelistAbandonCommand() *cobra.Command {
	var o surgeryBaseOptions
	abandonFreelistCmd := &cobra.Command{
		Use:   "abandon <bbolt-file>",
		Short: "Abandon the freelist from both meta pages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryFreelistAbandonFunc(args[0], o)
		},
	}
	o.AddFlags(abandonFreelistCmd.Flags())

	return abandonFreelistCmd
}

func surgeryFreelistAbandonFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[freelist abandon] copy file failed: %w", err)
	}

	if err := surgeon.ClearFreelist(cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("abandom-freelist command failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was abandoned in both meta pages.\nIt may cause some delay on next startup because bbolt needs to scan the whole db to reconstruct the free list.\n")
	return nil
}

func newSurgeryFreelistRebuildCommand() *cobra.Command {
	var o surgeryBaseOptions
	rebuildFreelistCmd := &cobra.Command{
		Use:   "rebuild <bbolt-file>",
		Short: "Rebuild the freelist",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryFreelistRebuildFunc(args[0], o)
		},
	}
	o.AddFlags(rebuildFreelistCmd.Flags())

	return rebuildFreelistCmd
}

func surgeryFreelistRebuildFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	// Ensure source file exists.
	fi, err := checkSourceDBPath(srcDBPath)
	if err != nil {
		return err
	}

	// make sure the freelist isn't present in the file.
	meta, err := readMetaPage(srcDBPath)
	if err != nil {
		return err
	}
	if meta.IsFreelistPersisted() {
		return ErrSurgeryFreelistAlreadyExist
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[freelist rebuild] copy file failed: %w", err)
	}

	// bboltDB automatically reconstruct & sync freelist in write mode.
	db, err := bolt.Open(cfg.outputDBFilePath, fi.Mode(), &bolt.Options{NoFreelistSync: false})
	if err != nil {
		return fmt.Errorf("[freelist rebuild] open db file failed: %w", err)
	}
	err = db.Close()
	if err != nil {
		return fmt.Errorf("[freelist rebuild] close db file failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was successfully rebuilt.\n")
	return nil
}

// defaultRebuildBatchPages bounds the work of a single rebuild-batched
// invocation; rerun the command to continue from the checkpoint.
const defaultRebuildBatchPages = 10000

type surgeryFreelistRebuildBatchedOptions struct {
	surgeryBaseOptions
	statePath  string
	batchPages int
}

func (o *surgeryFreelistRebuildBatchedOptions) AddFlags(fs *pflag.FlagSet) {
	o.surgeryBaseOptions.AddFlags(fs)
	fs.StringVar(&o.statePath, "state", o.statePath,
		"path to the resumable rebuild state file (defaults to <output>.rebuild-state.json)")
	fs.IntVar(&o.batchPages, "batch-pages", defaultRebuildBatchPages,
		"maximum number of pages processed per invocation (scan and sweep); <=0 processes everything")
}

func newSurgeryFreelistRebuildBatchedCommand() *cobra.Command {
	var o surgeryFreelistRebuildBatchedOptions
	rebuildBatchedCmd := &cobra.Command{
		Use:   "rebuild-batched <bbolt-file>",
		Short: "Rebuild the freelist in resumable batches based on actual page references",
		Long: `Rebuild the freelist in resumable batches based on actual page references.

The command scans the b-tree reachable from the active meta page in bounded
batches and classifies every page below the high water mark as reachable or
free. Progress is persisted in the state file after every batch, so the
command can be interrupted (including by a power failure) and rerun to
continue where it stopped.

The source database is only ever read. The output database (and the new
freelist inside it) is written exactly once, after the whole file has been
classified without disagreements. If referenced pages disagree with the
on-disk state (e.g. a live page is registered as free, a page is referenced
twice, or a page is malformed), the command stops, lists the offending page
ids and writes nothing.

Rerun the same command repeatedly until it reports success; rerunning a
finished rebuild is a no-op.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryFreelistRebuildBatchedFunc(args[0], o)
		},
	}
	o.AddFlags(rebuildBatchedCmd.Flags())

	return rebuildBatchedCmd
}

func surgeryFreelistRebuildBatchedFunc(srcDBPath string, cfg surgeryFreelistRebuildBatchedOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	progress, err := surgeon.RunRebuildFreelist(surgeon.RebuildFreelistOptions{
		SourcePath: srcDBPath,
		OutputPath: cfg.outputDBFilePath,
		StatePath:  cfg.statePath,
		BatchPages: cfg.batchPages,
	})
	if err != nil {
		return err
	}

	switch progress.Phase {
	case surgeon.RebuildPhaseDone:
		fmt.Fprintf(os.Stdout,
			"The freelist was successfully rebuilt: %d free page(s) registered, output written to %s.\n",
			progress.FreePages, cfg.outputDBFilePath)
	case surgeon.RebuildPhaseScanTree:
		fmt.Fprintf(os.Stdout,
			"Scanning reachable pages: %d page(s) confirmed reachable, %d page(s) queued; rerun the command to continue.\n",
			progress.ReachablePages, progress.PendingTreePages)
	case surgeon.RebuildPhaseSweep:
		fmt.Fprintf(os.Stdout,
			"Classifying pages: %d/%d page(s) swept, %d free page(s) found so far; rerun the command to continue.\n",
			progress.SweptPages, progress.TotalPages-2, progress.FreePages)
	case surgeon.RebuildPhaseApply:
		// The apply phase runs to completion within one invocation; report
		// progress for completeness if it ever surfaces.
		fmt.Fprintf(os.Stdout, "Writing rebuilt database to %s...\n", cfg.outputDBFilePath)
	default:
		fmt.Fprintf(os.Stdout, "Rebuild phase: %s; rerun the command to continue.\n", progress.Phase)
	}
	return nil
}
