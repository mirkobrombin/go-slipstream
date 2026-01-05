package wal

import (
	"encoding/binary"
	"fmt"
)

const (
	EntryPut      byte = 0
	EntryDelete   byte = 1
	EntryCommit   byte = 2
	EntryRollback byte = 3
	EntryLink     byte = 4
)

// Entry represents a single entry in the Write-Ahead Log.
type Entry struct {
	Type      byte
	TxID      uint64
	Key       string
	Value     []byte
	ExpiresAt int64
}

var (
	ErrSegmentFull = fmt.Errorf("wal: segment full")
	ErrClosed      = fmt.Errorf("wal: closed")
	ErrNotFound    = fmt.Errorf("wal: not found")
)

const (
	// SegmentSizeBytes is the default max size for segments.
	DefaultSegmentSize = 64 * 1024 * 1024
	// segmentShift determines bits for offset.
	segmentShift = 32
	offsetMask   = (1 << segmentShift) - 1
)

// PackOffset combines segment ID and file offset into a single int64.
func PackOffset(segmentID uint64, offset int64) int64 {
	return int64((segmentID << segmentShift) | uint64(offset))
}

func UnpackOffset(packed int64) (uint64, int64) {
	id := uint64(packed) >> segmentShift
	offset := packed & offsetMask
	return id, offset
}

// EncodeEntry binary encodes an entry.
// [Type:1][TxID:8][ExpiresAt:8][KeyLen:4][Key:N][ValueLen:4][Value:M]
func EncodeEntry(e Entry) []byte {
	keyLen := len(e.Key)
	valLen := len(e.Value)
	buf := make([]byte, 1+8+8+4+keyLen+4+valLen)

	buf[0] = e.Type
	binary.BigEndian.PutUint64(buf[1:], e.TxID)
	binary.BigEndian.PutUint64(buf[9:], uint64(e.ExpiresAt))
	binary.BigEndian.PutUint32(buf[17:], uint32(keyLen))
	copy(buf[21:], e.Key)

	vOffset := 21 + keyLen
	binary.BigEndian.PutUint32(buf[vOffset:], uint32(valLen))
	copy(buf[vOffset+4:], e.Value)

	return buf
}
