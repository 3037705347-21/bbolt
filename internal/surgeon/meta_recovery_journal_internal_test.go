package surgeon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveJournalAtomicLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.json")

	j := &recoveryJournal{
		Current: &journalEpisode{Status: statusCompleted, Events: []journalEvent{{Time: "now", Type: eventPhase, Message: "done"}}},
	}
	require.NoError(t, saveJournalAtomic(path, j))

	loaded, exists, err := loadJournal(path)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, statusCompleted, loaded.Current.Status)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp", "no temp file may remain next to the journal")
	}
}

func TestSaveJournalAtomicCrashBeforeRenameKeepsPreviousVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.json")
	tmpPath := filepath.Join(dir, "."+filepath.Base(path)+".tmp")

	first := &recoveryJournal{Current: &journalEpisode{Status: statusDecided}}
	require.NoError(t, saveJournalAtomic(path, first))

	// Simulate a process that wrote the temp file but crashed before rename.
	require.NoError(t, os.WriteFile(tmpPath, []byte("{partial-garbage"), 0o600))

	loaded, exists, err := loadJournal(path)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, statusDecided, loaded.Current.Status, "committed journal must survive an abandoned temp file")

	// A new successful save clears the stale temp file atomically.
	second := &recoveryJournal{Current: &journalEpisode{Status: statusCompleted}}
	require.NoError(t, saveJournalAtomic(path, second))
	loaded, exists, err = loadJournal(path)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, statusCompleted, loaded.Current.Status)
	_, statErr := os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(statErr))
}

func TestBeginEpisodePreservesHistory(t *testing.T) {
	j := &recoveryJournal{SchemaVersion: journalSchemaVersion}
	ep1 := &journalEpisode{Status: statusCompleted}
	ep2 := &journalEpisode{Status: statusDecided}
	j.beginEpisode(ep1)
	j.beginEpisode(ep2)
	require.Len(t, j.History, 1)
	assert.Equal(t, ep1, j.History[0])
	assert.Equal(t, ep2, j.Current)
}
