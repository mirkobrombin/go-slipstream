//go:build linux

package wal

import "syscall"

func directOpenFlag() int {
	return syscall.O_DIRECT
}
