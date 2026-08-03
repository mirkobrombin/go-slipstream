//go:build !linux

package wal

func directOpenFlag() int {
	return 0
}
