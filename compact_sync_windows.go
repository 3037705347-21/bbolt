//go:build windows

package bbolt

// fsyncDir is a no-op on Windows: there is no portable API to fsync a
// directory handle. os.Rename within the same volume is atomic on NTFS, which
// is all the resumable compaction relies on.
func fsyncDir(dirPath string) error {
	return nil
}
