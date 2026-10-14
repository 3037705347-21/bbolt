package command

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type compactOptions struct {
	dstPath   string
	txMaxSize int64
	dstNoSync bool

	// Resumable compaction options.
	resumable bool
	batchSize int64
	restart   bool
}

func newCompactCommand() *cobra.Command {
	var o compactOptions
	var compactCmd = &cobra.Command{
		Use:   "compact [options] -o <dst-bbolt-file> <src-bbolt-file>",
		Short: "creates a compacted copy of the database from source path to the destination path, preserving the original.",
		Long: `compact opens a database at source path and walks it recursively, copying keys
as they are found from all buckets, to a newly created database at the destination path.
The original database is left untouched.

With --resume, the destination is built in a temporary file and progress is
checkpointed after each batch. An interrupted run (process exit, machine
reboot) is continued from the last checkpoint when the command is executed
again with the same arguments. The destination file is only created after the
full copy has been verified to be identical to the source snapshot. If the
source database has changed since the interrupted run, the partial output is
discarded and the command fails; run it again to compact the current source.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(args[0]); err != nil {
				return err
			}
			return o.Run(cmd, args[0])
		},
	}
	o.AddFlags(compactCmd.Flags())

	return compactCmd
}

func (o *compactOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.dstPath, "output", "o", "", "")
	fs.Int64Var(&o.txMaxSize, "tx-max-size", 65536, "")
	fs.BoolVar(&o.dstNoSync, "no-sync", false, "")
	fs.BoolVar(&o.resumable, "resume", false, "checkpoint progress while compacting so an interrupted run resumes when re-invoked")
	fs.Int64Var(&o.batchSize, "batch-size", 64*1024*1024, "with --resume: amount of source data (in bytes) copied between durable checkpoints")
	fs.BoolVar(&o.restart, "restart", false, "with --resume: discard a previous checkpoint and partial output and compact from scratch")
	_ = cobra.MarkFlagRequired(fs, "output")
}

func (o *compactOptions) Validate(srcPath string) (err error) {
	if o.dstPath == "" {
		return errors.New("output file required")
	}
	if o.resumable && o.dstNoSync {
		return errors.New("--no-sync cannot be used with --resume: durable checkpoints require syncing")
	}
	if o.batchSize < 0 {
		return errors.New("--batch-size must be greater than zero")
	}

	return
}

func (o *compactOptions) Run(cmd *cobra.Command, srcPath string) (err error) {

	// ensure source file exists.
	fi, err := checkSourceDBPath(srcPath)
	if err != nil {
		return err
	}
	initialSize := fi.Size()

	if o.resumable {
		if err := bolt.CompactResumable(srcPath, o.dstPath, fi.Mode(), &bolt.ResumableCompactOptions{
			BatchSize: o.batchSize,
			Restart:   o.restart,
		}); err != nil {
			return err
		}
	} else {
		if o.restart {
			return errors.New("--restart can only be used with --resume")
		}

		// open source database.
		src, err := bolt.Open(srcPath, 0400, &bolt.Options{ReadOnly: true})
		if err != nil {
			return err
		}
		defer src.Close()

		// open destination database.
		dst, err := bolt.Open(o.dstPath, fi.Mode(), &bolt.Options{NoSync: o.dstNoSync})
		if err != nil {
			return err
		}
		defer dst.Close()

		// run compaction.
		if err := bolt.Compact(dst, src, o.txMaxSize); err != nil {
			return err
		}
	}

	// report stats on new size.
	fi, err = os.Stat(o.dstPath)
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		return fmt.Errorf("zero db size")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%d -> %d bytes (gain=%.2fx)\n", initialSize, fi.Size(), float64(initialSize)/float64(fi.Size()))

	return
}
