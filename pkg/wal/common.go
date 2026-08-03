package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	ErrSegmentFull     = fmt.Errorf("wal: segment full")
	ErrClosed          = fmt.Errorf("wal: closed")
	ErrNotFound        = fmt.Errorf("wal: not found")
	ErrCorrupt         = errors.New("wal: corrupt entry")
	ErrDirectoryLocked = errors.New("wal: directory is already open")
	ErrEntryTooLarge   = errors.New("wal: entry exceeds encoding limits")
	ErrTornWrite       = errors.New("wal: torn active-segment write")
)

const (
	checksummedEntryFlag        byte = 0x80
	checksummedHeaderSize            = 29
	checksummedMinimumEntrySize      = 37
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// CorruptionError identifies a malformed WAL record.
type CorruptionError struct {
	SegmentID uint64
	Offset    int64
	Cause     error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corrupt entry in segment %d at offset %d: %v", e.SegmentID, e.Offset, e.Cause)
}

func (e *CorruptionError) Unwrap() error {
	return e.Cause
}

func (e *CorruptionError) Is(target error) bool {
	return target == ErrCorrupt || errors.Is(e.Cause, target)
}

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

// EncodeEntry binary encodes a checksummed entry. The decoder also accepts the
// legacy unframed format written before v1.1.0.
func EncodeEntry(e Entry) []byte {
	keyLen := len(e.Key)
	valLen := len(e.Value)
	totalLen := checksummedMinimumEntrySize + keyLen + valLen
	buf := make([]byte, totalLen)

	buf[0] = e.Type | checksummedEntryFlag
	binary.BigEndian.PutUint32(buf[1:], uint32(totalLen))
	binary.BigEndian.PutUint64(buf[5:], e.TxID)
	binary.BigEndian.PutUint64(buf[13:], uint64(e.ExpiresAt))
	binary.BigEndian.PutUint32(buf[21:], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[25:], crc32.Checksum(buf[:25], checksumTable))
	copy(buf[checksummedHeaderSize:], e.Key)

	vOffset := checksummedHeaderSize + keyLen
	binary.BigEndian.PutUint32(buf[vOffset:], uint32(valLen))
	copy(buf[vOffset+4:], e.Value)
	binary.BigEndian.PutUint32(buf[totalLen-4:], crc32.Checksum(buf[:totalLen-4], checksumTable))

	return buf
}
