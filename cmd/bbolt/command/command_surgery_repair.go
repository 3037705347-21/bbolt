package command

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/surgeon"
)

// newSurgeryRepairCommand groups the crash-safe multi-page repair flow:
//
//	repair begin      copy the source db and create a repair journal
//	repair copy-page apply one page write (journaled, crash safe)
//	repair recover    roll back unfinished pages / redo damaged ones
//	repair status     inspect what was recorded for every page
//	repair verify     confirm finished pages still belong to this version
//	repair finish     verify, finalize and remove the journal
func newSurgeryRepairCommand() *cobra.Command {
	repairCmd := &cobra.Command{
		Use:   "repair <subcommand>",
		Short: "Crash safe, journaled multi-page repair workflow",
	}
	repairCmd.AddCommand(
		newSurgeryRepairBeginCommand(),
		newSurgeryRepairCopyPageCommand(),
		newSurgeryRepairRecoverCommand(),
		newSurgeryRepairStatusCommand(),
		newSurgeryRepairVerifyCommand(),
		newSurgeryRepairFinishCommand(),
	)
	return repairCmd
}

func newSurgeryRepairBeginCommand() *cobra.Command {
	var o surgeryBaseOptions
	cmd := &cobra.Command{
		Use:   "begin <bbolt-file>",
		Short: "Copy the source database and start a journaled repair session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryRepairBeginFunc(args[0], o)
		},
	}
	o.AddFlags(cmd.Flags())
	return cmd
}

func surgeryRepairBeginFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}
	s, err := surgeon.BeginRepair(srcDBPath, cfg.outputDBFilePath)
	if err != nil {
		return fmt.Errorf("begin repair failed: %w", err)
	}
	st := s.Status()
	if err := s.Close(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Repair session %d started\n", st.SessionID)
	fmt.Fprintf(os.Stdout, "  working database: %s\n", cfg.outputDBFilePath)
	fmt.Fprintf(os.Stdout, "  journal:          %s\n", surgeon.JournalPath(cfg.outputDBFilePath))
	fmt.Fprintf(os.Stdout, "  source txid:      %d\n", st.ActiveTxid)
	fmt.Fprintln(os.Stdout, "Apply page writes with `surgery repair copy-page`, then `surgery repair finish`.")
	return nil
}

type surgeryRepairCopyPageOptions struct {
	sourcePageId      uint64
	destinationPageId uint64
}

func (o *surgeryRepairCopyPageOptions) AddFlags(fs *pflag.FlagSet) {
	fs.Uint64VarP(&o.sourcePageId, "from-page", "", o.sourcePageId, "source page Id")
	fs.Uint64VarP(&o.destinationPageId, "to-page", "", o.destinationPageId, "destination page Id")
	_ = cobra.MarkFlagRequired(fs, "from-page")
	_ = cobra.MarkFlagRequired(fs, "to-page")
}

func (o *surgeryRepairCopyPageOptions) Validate() error {
	if o.sourcePageId == o.destinationPageId {
		return fmt.Errorf("'--from-page' and '--to-page' have the same value: %d", o.sourcePageId)
	}
	return nil
}

func newSurgeryRepairCopyPageCommand() *cobra.Command {
	var o surgeryRepairCopyPageOptions
	cmd := &cobra.Command{
		Use:   "copy-page <repaired-bbolt-file>",
		Short: "Copy a page within the repaired database, recording before/after images",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryRepairCopyPageInSessionFunc(args[0], o)
		},
	}
	o.AddFlags(cmd.Flags())
	return cmd
}

func surgeryRepairCopyPageInSessionFunc(dbPath string, cfg surgeryRepairCopyPageOptions) error {
	// Resuming also recovers any page write interrupted by a crash.
	s, report, err := surgeon.ResumeRepair(dbPath)
	if err != nil {
		return fmt.Errorf("open repair session failed: %w", err)
	}
	printRecoveryReport(report)
	defer s.Close()

	if err := s.CopyPage(common.Pgid(cfg.sourcePageId), common.Pgid(cfg.destinationPageId)); err != nil {
		return fmt.Errorf("copy-page failed: %w", err)
	}
	fmt.Fprintf(os.Stdout, "Page %d copied to page %d and committed to the repair journal\n",
		cfg.sourcePageId, cfg.destinationPageId)
	return nil
}

func newSurgeryRepairRecoverCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "recover <repaired-bbolt-file>",
		Short: "Recover interrupted page writes: roll back unfinished pages, redo damaged ones",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, report, err := surgeon.ResumeRepair(args[0])
			if err != nil {
				return fmt.Errorf("recovery failed: %w", err)
			}
			printRecoveryReport(report)
			return s.Close()
		},
	}
}

func newSurgeryRepairStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <repaired-bbolt-file>",
		Short: "Show recorded page repairs and their state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := surgeon.OpenRepair(args[0])
			if err != nil {
				return fmt.Errorf("open repair session failed: %w", err)
			}
			defer s.Close()
			printRepairStatus(s.Status())
			return nil
		},
	}
}

func newSurgeryRepairVerifyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "verify <repaired-bbolt-file>",
		Short: "Verify every repaired page and that the file still matches the repaired version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := surgeon.OpenRepair(args[0])
			if err != nil {
				return fmt.Errorf("open repair session failed: %w", err)
			}
			defer s.Close()
			if err := s.Verify(); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "OK: all repaired pages match the committed images and the repaired version identity is intact")
			return nil
		},
	}
}

func newSurgeryRepairFinishCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "finish <repaired-bbolt-file>",
		Short: "Verify, finalize the repair session and remove its journal",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, report, err := surgeon.ResumeRepair(args[0])
			if err != nil {
				return fmt.Errorf("open repair session failed: %w", err)
			}
			printRecoveryReport(report)
			if err := s.Finish(); err != nil {
				return fmt.Errorf("finish repair failed: %w", err)
			}
			fmt.Fprintln(os.Stdout, "Repair finished and verified; journal removed. The database is ready for the final check.")
			return nil
		},
	}
}

func printRecoveryReport(report *surgeon.RecoveryReport) {
	if report == nil {
		return
	}
	for _, p := range report.RolledBack {
		fmt.Fprintf(os.Stdout, "recovered: rolled back page %d (%s, seq=%d) to its original content\n",
			p.Pgid, p.Op, p.Seq)
	}
	for _, p := range report.Aborted {
		fmt.Fprintf(os.Stdout, "recovered: aborted unfinished repair of page %d (%s, seq=%d)\n",
			p.Pgid, p.Op, p.Seq)
	}
	for _, p := range report.Redone {
		fmt.Fprintf(os.Stdout, "recovered: redid page %d (%s, seq=%d) from its committed image\n",
			p.Pgid, p.Op, p.Seq)
	}
	if len(report.RolledBack) == 0 && len(report.Aborted) == 0 && len(report.Redone) == 0 {
		fmt.Fprintln(os.Stdout, "No interrupted page repair found.")
	}
}

func printRepairStatus(st surgeon.RepairStatus) {
	fmt.Fprintf(os.Stdout, "Repair session: %d\n", st.SessionID)
	fmt.Fprintf(os.Stdout, "Source:         %s\n", st.SourcePath)
	fmt.Fprintf(os.Stdout, "Created:        %s\n", st.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(os.Stdout, "Page size:      %d\n", st.PageSize)
	fmt.Fprintf(os.Stdout, "File size:      %d\n", st.SourceFileSize)
	fmt.Fprintf(os.Stdout, "Source txid:    %d\n", st.ActiveTxid)
	if st.Finalized {
		fmt.Fprintln(os.Stdout, "State:          finalized")
	} else {
		fmt.Fprintln(os.Stdout, "State:          open")
	}
	if len(st.Pages) == 0 {
		fmt.Fprintln(os.Stdout, "No page repair recorded yet.")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SEQ\tPAGE\tOPERATION\tSTATE\tBEFORE-SHA256\tAFTER-SHA256\tBEFORE\tAFTER")
	for _, p := range st.Pages {
		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d\n",
			p.Seq, p.Pgid, p.Op, p.State, p.BeforeHash, p.AfterHash, p.BeforeSize, p.AfterSize)
	}
	_ = w.Flush()
}
