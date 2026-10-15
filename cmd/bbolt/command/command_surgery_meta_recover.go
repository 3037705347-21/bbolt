package command

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.etcd.io/bbolt/internal/surgeon"
)

type surgeryMetaRecoverOptions struct {
	surgeryBaseOptions
	journalPath    string
	selectMetaPage int
}

// AddFlags sets the flags for `meta recover` command.
func (o *surgeryMetaRecoverOptions) AddFlags(fs *pflag.FlagSet) {
	o.surgeryBaseOptions.AddFlags(fs)
	fs.StringVar(&o.journalPath, "journal", o.journalPath, "path of the JSON decision journal that records the selected meta page, evidence and rollback events (defaults to <output>.meta-recovery.json)")
	fs.IntVar(&o.selectMetaPage, "select-meta-page", -1, "explicitly choose meta page 0 or 1; only allowed when both pages are valid with the same txid but different contents")
}

func (o *surgeryMetaRecoverOptions) Validate() error {
	if err := o.surgeryBaseOptions.Validate(); err != nil {
		return err
	}
	if o.selectMetaPage != -1 && o.selectMetaPage != 0 && o.selectMetaPage != 1 {
		return fmt.Errorf("invalid --select-meta-page %d: must be 0 or 1", o.selectMetaPage)
	}
	return nil
}

func newSurgeryMetaRecoverCommand() *cobra.Command {
	var o surgeryMetaRecoverOptions
	metaRecoverCmd := &cobra.Command{
		Use:   "recover <bbolt-file>",
		Short: "Deterministically recover diverged meta pages after a crash, recording the decision in a journal",
		Long: `Inspect both meta pages of the (offline) source database, deterministically choose a
version and write a repaired copy to --output. The source file is never modified.

When both meta pages are invalid, or both are valid with the same txid but
different contents, the command stops without modifying anything; the latter
case requires an explicit --select-meta-page 0|1. The decision, evidence,
rollback events and pre/post fingerprints are recorded in a JSON journal, so
an interrupted re-run converges to the exact same conclusion.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryMetaRecoverFunc(cmd, args[0], o)
		},
	}
	o.AddFlags(metaRecoverCmd.Flags())
	return metaRecoverCmd
}

func surgeryMetaRecoverFunc(cmd *cobra.Command, srcDBPath string, cfg surgeryMetaRecoverOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	journalPath := cfg.journalPath
	if journalPath == "" {
		journalPath = cfg.outputDBFilePath + ".meta-recovery.json"
	}

	var opts surgeon.RecoverOptions
	if cfg.selectMetaPage != -1 {
		forced := cfg.selectMetaPage
		opts.ForcedPage = &forced
	}

	report, err := surgeon.RecoverMetaPages(srcDBPath, cfg.outputDBFilePath, journalPath, opts)
	if err != nil {
		switch {
		case errors.Is(err, surgeon.ErrBothMetaPagesInvalid):
			return fmt.Errorf("recovery aborted because both meta pages are invalid; the source file was not modified and no output was produced: %w", err)
		case errors.Is(err, surgeon.ErrMetaPagesAmbiguous):
			return fmt.Errorf("recovery aborted because the two valid meta pages cannot be distinguished; re-run with --select-meta-page 0|1: %w", err)
		case errors.Is(err, surgeon.ErrRecoveryStateMismatch):
			return fmt.Errorf("recovery aborted because the output file no longer matches the state recorded in %q: %w", journalPath, err)
		default:
			return err
		}
	}

	printRecoveryReport(cmd, report, journalPath)
	return nil
}

func printRecoveryReport(cmd *cobra.Command, report *surgeon.RecoveryReport, journalPath string) {
	out := cmd.OutOrStdout()
	if report.SelectedPage < 0 {
		fmt.Fprintf(out, "Both meta pages are identical (txid %d); no repair was needed.\n", report.SelectedTxid)
	} else {
		fmt.Fprintf(out, "Selected meta page %d (txid %d), reason: %s\n", report.SelectedPage, report.SelectedTxid, report.Reason)
	}
	if report.Rollback != nil {
		fmt.Fprintf(out, "Rollback: discarded page %d (txid %d), kept page %d (txid %d)\n  %s\n",
			report.Rollback.DiscardedPage, report.Rollback.DiscardedTxid,
			report.Rollback.KeptPage, report.Rollback.KeptTxid, report.Rollback.Reason)
	}
	if report.Action.Type == "none" {
		fmt.Fprintln(out, "Action: none")
	} else {
		fmt.Fprintf(out, "Action: copied meta page %d over page %d in the output copy\n", report.Action.From, report.Action.To)
	}
	if report.Resumed && report.AlreadyConsistent {
		fmt.Fprintln(out, "The output already matched the recorded decision; nothing was changed (idempotent re-run).")
	} else if report.Resumed {
		fmt.Fprintln(out, "Resumed from the existing journal and converged to the same decision.")
	}
	for _, backup := range report.BackupPaths {
		fmt.Fprintf(out, "Original meta page backup: %s\n", backup)
	}
	fmt.Fprintf(out, "Recovered database: %s\n", report.OutputPath)
	fmt.Fprintf(out, "Decision journal: %s\n", journalPath)
}
