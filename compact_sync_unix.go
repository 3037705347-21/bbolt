//go:build !windows

package bbolt

import "os"

// fsyncDir flushes the directory entry at dirPath to stable storage so that a
// recently renamed or created file becomes durable. On Unix systems opening a
// directory and fsync-ing its file descriptor is the standard way to do this.
func fsyncDir(dirPath string) error {
	f, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
