//go:build windows

package wal

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func lockDirectory(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	var overlapped windows.Overlapped
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrDirectoryLocked
		}
		return nil, err
	}
	return f, nil
}

func unlockDirectory(f *os.File) error {
	if f == nil {
		return nil
	}
	var overlapped windows.Overlapped
	err := windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &overlapped)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
