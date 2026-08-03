package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Manager handles a collection of WAL segments.
type Manager struct {
	mu            sync.RWMutex
	dir           string
	active        *Segment
	sealed        []*Segment
	txID          uint64
	maxSize       int64
	directoryLock *os.File
	closed        bool
}

// NewManager creates a new WAL manager.
func NewManager(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	directoryLock, err := lockDirectory(filepath.Join(dir, ".slipstream.lock"))
	if err != nil {
		return nil, fmt.Errorf("wal: lock %s: %w", dir, err)
	}

	m := &Manager{
		dir:           dir,
		maxSize:       DefaultSegmentSize,
		txID:          uint64(time.Now().UnixNano()),
		directoryLock: directoryLock,
	}

	if err := m.loadSegments(); err != nil {
		_ = m.Close()
		return nil, err
	}
	if err := m.restoreTxID(); err != nil {
		_ = m.Close()
		return nil, err
	}

	return m, nil
}

func (m *Manager) loadSegments() error {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}

	var segmentIDs []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}

		name := strings.TrimSuffix(e.Name(), ".log")
		id, err := strconv.ParseUint(name, 16, 64)
		if err != nil {
			continue // Skip malformed files
		}
		segmentIDs = append(segmentIDs, id)
	}

	sort.Slice(segmentIDs, func(i, j int) bool {
		return segmentIDs[i] < segmentIDs[j]
	})

	for _, id := range segmentIDs {
		path := filepath.Join(m.dir, fmt.Sprintf("%016x.log", id))
		seg, err := NewSegment(id, path, m.maxSize)
		if err != nil {
			return err
		}

		if len(segmentIDs) > 0 && id == segmentIDs[len(segmentIDs)-1] {
			m.active = seg
		} else {
			if err := seg.Close(); err != nil {
				return err
			}
			m.sealed = append(m.sealed, seg)
		}
	}

	if m.active == nil {
		path := filepath.Join(m.dir, fmt.Sprintf("%016x.log", 0))
		seg, err := NewSegment(0, path, m.maxSize)
		if err != nil {
			return err
		}
		m.active = seg
	}

	return nil
}

// Append writes an entry to the active segment, rotating if necessary.
func (m *Manager) Append(entry Entry) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	encodedSize := uint64(checksummedMinimumEntrySize) + uint64(len(entry.Key)) + uint64(len(entry.Value))
	if uint64(len(entry.Key)) > math.MaxUint32 || uint64(len(entry.Value)) > math.MaxUint32 || encodedSize > math.MaxUint32 {
		return 0, ErrEntryTooLarge
	}

	data := EncodeEntry(entry)

	// Check rotation
	if m.active.Size()+int64(len(data)) > m.maxSize {
		if err := m.rotate(); err != nil {
			return 0, err
		}
	}

	offset, err := m.active.Write(data)
	if err != nil {
		return 0, err
	}

	return PackOffset(m.active.ID(), offset), nil
}

func (m *Manager) rotate() error {
	if err := m.active.Sync(); err != nil {
		return err
	}
	old := m.active
	newID := old.ID() + 1
	path := filepath.Join(m.dir, fmt.Sprintf("%016x.log", newID))
	seg, err := NewSegment(newID, path, m.maxSize)
	if err != nil {
		return err
	}
	if err := old.Close(); err != nil {
		_ = seg.Close()
		_ = os.Remove(path)
		return err
	}
	m.sealed = append(m.sealed, old)
	m.active = seg
	return nil
}

// ReadAt reads from the correct segment and unpacks the entry value.
func (m *Manager) ReadAt(packedOffset int64) ([]byte, error) {
	entry, err := m.ReadEntryAt(packedOffset)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (m *Manager) ReadEntryAt(packedOffset int64) (Entry, error) {
	segID, offset := UnpackOffset(packedOffset)

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return Entry{}, ErrClosed
	}
	var target *Segment
	if m.active.ID() == segID {
		target = m.active
	} else {
		for _, s := range m.sealed {
			if s.ID() == segID {
				target = s
				break
			}
		}
	}
	if target == nil {
		return Entry{}, ErrNotFound
	}

	return m.readEntry(target, offset)
}

func (m *Manager) readEntry(seg *Segment, offset int64) (Entry, error) {
	entry, _, err := decodeEntryAt(seg, offset)
	return entry, err
}

func decodeEntryAt(seg *Segment, offset int64) (Entry, int64, error) {
	corrupt := func(cause error) (Entry, int64, error) {
		return Entry{}, offset, &CorruptionError{SegmentID: seg.ID(), Offset: offset, Cause: cause}
	}
	size := seg.Size()
	if offset < 0 || offset >= size {
		return corrupt(io.ErrUnexpectedEOF)
	}
	first, err := seg.ReadAt(offset, 1)
	if err != nil {
		return corrupt(err)
	}
	if first[0]&checksummedEntryFlag != 0 {
		return decodeChecksummedEntryAt(seg, offset)
	}
	if size-offset < 21 {
		return corrupt(io.ErrUnexpectedEOF)
	}
	header, err := seg.ReadAt(offset, 21)
	if err != nil {
		return corrupt(err)
	}
	keyLen := int64(binary.BigEndian.Uint32(header[17:]))
	keyStart := offset + 21
	valueLenStart := keyStart + keyLen
	maxInt := int64(^uint(0) >> 1)
	if keyLen > maxInt || keyLen > size-keyStart || size-valueLenStart < 4 {
		return corrupt(io.ErrUnexpectedEOF)
	}
	key, err := seg.ReadAt(keyStart, int(keyLen))
	if err != nil {
		return corrupt(err)
	}
	valueLenBytes, err := seg.ReadAt(valueLenStart, 4)
	if err != nil {
		return corrupt(err)
	}
	valueLen := int64(binary.BigEndian.Uint32(valueLenBytes))
	valueStart := valueLenStart + 4
	if valueLen > maxInt || valueLen > size-valueStart {
		return corrupt(io.ErrUnexpectedEOF)
	}
	value, err := seg.ReadAt(valueStart, int(valueLen))
	if err != nil {
		return corrupt(err)
	}
	next := valueStart + valueLen
	return Entry{
		Type:      header[0],
		TxID:      binary.BigEndian.Uint64(header[1:]),
		ExpiresAt: int64(binary.BigEndian.Uint64(header[9:])),
		Key:       string(key),
		Value:     value,
	}, next, nil
}

func decodeChecksummedEntryAt(seg *Segment, offset int64) (Entry, int64, error) {
	corrupt := func(cause error) (Entry, int64, error) {
		return Entry{}, offset, &CorruptionError{SegmentID: seg.ID(), Offset: offset, Cause: cause}
	}
	size := seg.Size()
	remaining := size - offset
	if remaining < checksummedHeaderSize {
		return corrupt(ErrTornWrite)
	}
	header, err := seg.ReadAt(offset, checksummedHeaderSize)
	if err != nil {
		return corrupt(err)
	}
	wantHeaderChecksum := binary.BigEndian.Uint32(header[25:])
	if crc32.Checksum(header[:25], checksumTable) != wantHeaderChecksum {
		return corrupt(fmt.Errorf("header checksum mismatch"))
	}
	totalLen := int64(binary.BigEndian.Uint32(header[1:]))
	keyLen := int64(binary.BigEndian.Uint32(header[21:]))
	if totalLen < checksummedMinimumEntrySize || keyLen > totalLen-checksummedMinimumEntrySize {
		return corrupt(fmt.Errorf("invalid framed lengths"))
	}
	if totalLen > remaining {
		return corrupt(ErrTornWrite)
	}
	maxInt := int64(^uint(0) >> 1)
	if totalLen > maxInt {
		return corrupt(fmt.Errorf("entry length exceeds platform limit"))
	}
	record, err := seg.ReadAt(offset, int(totalLen))
	if err != nil {
		return corrupt(err)
	}
	wantRecordChecksum := binary.BigEndian.Uint32(record[totalLen-4:])
	if crc32.Checksum(record[:totalLen-4], checksumTable) != wantRecordChecksum {
		return corrupt(fmt.Errorf("record checksum mismatch"))
	}
	valueLenStart := int64(checksummedHeaderSize) + keyLen
	valueLen := int64(binary.BigEndian.Uint32(record[valueLenStart : valueLenStart+4]))
	valueStart := valueLenStart + 4
	valueEnd := valueStart + valueLen
	if valueLen > maxInt || valueEnd+4 != totalLen {
		return corrupt(fmt.Errorf("invalid value length"))
	}
	return Entry{
		Type:      record[0] &^ checksummedEntryFlag,
		TxID:      binary.BigEndian.Uint64(record[5:]),
		ExpiresAt: int64(binary.BigEndian.Uint64(record[13:])),
		Key:       string(record[checksummedHeaderSize:valueLenStart]),
		Value:     append([]byte(nil), record[valueStart:valueEnd]...),
	}, offset + totalLen, nil
}

func (m *Manager) restoreTxID() error {
	inspect := func(entry Entry, _ int64) error {
		if entry.TxID > m.txID {
			m.txID = entry.TxID
		}
		return nil
	}
	for _, segment := range m.sealed {
		if err := m.IterateSegment(segment, inspect); err != nil {
			return err
		}
	}
	return m.IterateActiveSegment(inspect)
}

func (m *Manager) NextTxID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txID++
	return m.txID
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true

	var firstErr error
	if m.active != nil {
		if err := m.active.Close(); err != nil {
			firstErr = err
		}
	}
	for _, s := range m.sealed {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := unlockDirectory(m.directoryLock); err != nil && firstErr == nil {
		firstErr = err
	}
	m.directoryLock = nil
	return firstErr
}

func (m *Manager) ActiveSegmentID() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active.ID()
}

func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return m.active.Sync()
}

func (m *Manager) SealedSegments() []*Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Return copy to be safe?
	cp := make([]*Segment, len(m.sealed))
	copy(cp, m.sealed)
	return cp
}

func (m *Manager) RemoveSegment(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, s := range m.sealed {
		if s.ID() == id {
			idx = i
			break
		}
	}

	if idx == -1 {
		return ErrNotFound
	}

	seg := m.sealed[idx]
	// Close file if open
	if err := seg.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "slipstream: failed to close segment %d: %v\n", seg.ID(), err)
	}

	// Remove from list
	m.sealed = append(m.sealed[:idx], m.sealed[idx+1:]...)

	// Remove file
	return os.Remove(seg.path)
}

func (m *Manager) IterateSegment(seg *Segment, fn func(e Entry, offset int64) error) error {
	offset := int64(0)
	size := seg.Size()

	for offset < size {
		entry, next, err := decodeEntryAt(seg, offset)
		if err != nil {
			return err
		}
		if err := fn(entry, PackOffset(seg.ID(), offset)); err != nil {
			return err
		}
		offset = next
	}
	return nil
}

func (m *Manager) SetMaxSegmentSize(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxSize = size
}

// Rotate seals the active segment and starts a new one.
func (m *Manager) Rotate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return m.rotate()
}

func (m *Manager) IterateActiveSegment(fn func(e Entry, offset int64) error) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	type item struct {
		entry  Entry
		offset int64
	}
	items := make([]item, 0)
	offset := int64(0)
	size := m.active.Size()
	for offset < size {
		entry, next, err := decodeEntryAt(m.active, offset)
		if err != nil {
			if errors.Is(err, ErrTornWrite) {
				if truncateErr := m.active.Truncate(offset); truncateErr != nil {
					m.mu.Unlock()
					return truncateErr
				}
				break
			}
			m.mu.Unlock()
			return err
		}
		items = append(items, item{entry: entry, offset: PackOffset(m.active.ID(), offset)})
		offset = next
	}
	m.mu.Unlock()
	for _, item := range items {
		if err := fn(item.entry, item.offset); err != nil {
			return err
		}
	}
	return nil
}
