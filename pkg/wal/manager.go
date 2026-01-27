package wal

import (
	"encoding/binary"
	"fmt"
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
	mu          sync.RWMutex
	dir         string
	active      *Segment
	sealed      []*Segment
	txID        uint64
	maxSize     int64
	readBufPool *sync.Pool
}

// NewManager creates a new WAL manager.
func NewManager(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		dir:     dir,
		maxSize: DefaultSegmentSize,
		txID:    uint64(time.Now().UnixNano()),
		readBufPool: &sync.Pool{
			New: func() any {
				// 4KB buffer for speculative reads (page aligned)
				b := make([]byte, 4096)
				return &b
			},
		},
	}

	if err := m.loadSegments(); err != nil {
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
			seg.Close()
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
	m.active.Close()

	m.sealed = append(m.sealed, m.active)

	newID := m.active.ID() + 1
	path := filepath.Join(m.dir, fmt.Sprintf("%016x.log", newID))

	seg, err := NewSegment(newID, path, m.maxSize)
	if err != nil {
		return err
	}
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
	m.mu.RUnlock()

	if target == nil {
		return Entry{}, ErrNotFound
	}

	return m.readEntry(target, offset)
}

func (m *Manager) readEntry(seg *Segment, offset int64) (Entry, error) {
	// Speculative read: fetch 4KB to avoid multiple syscalls
	bufPtr := m.readBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer m.readBufPool.Put(bufPtr)

	// Read up to buffer size or EOF (we don't know EOF yet, but ReadAt returns what it gets)
	// We should just read 4KB. Segment logic handles boundaries?
	// Segment.ReadAtBuffer uses file.ReadAt. file.ReadAt returns EOF if we read past.
	// We need to know segment size or handle short read.
	// But we passed offset.

	// We try to read header (21 bytes) + some data.
	n, err := seg.ReadAtBuffer(buf, offset)
	if err != nil && n < 21 { // Need at least header
		return Entry{}, err
	}

	if n < 21 {
		return Entry{}, fmt.Errorf("wal: short read for header")
	}

	// [Type:1][TxID:8][ExpiresAt:8][KeyLen:4][Key:N][ValueLen:4][Value:M]
	entryType := buf[0]
	txID := binary.BigEndian.Uint64(buf[1:])
	expiresAt := int64(binary.BigEndian.Uint64(buf[9:]))
	if expiresAt > 0 && time.Now().UnixNano() > expiresAt {
		return Entry{}, ErrNotFound
	}

	keyLen := binary.BigEndian.Uint32(buf[17:])

	totalKeyOffset := 21 + int64(keyLen)
	// Do we have key in buffer?
	// buf has 'n' bytes.
	// We need 21 + keyLen <= n

	var key string
	if int64(n) >= totalKeyOffset {
		// Key is in buffer
		key = string(buf[21:totalKeyOffset])
	} else {
		// Key is partial or outside. Read explicitly.
		kBuf := make([]byte, keyLen) // Allocation unavoidable for string conversion anyway? string(buf) copies.
		// Use ReadAt for key
		if _, err := seg.ReadAtBuffer(kBuf, offset+21); err != nil {
			return Entry{}, err
		}
		key = string(kBuf)
	}

	// Value Length
	// Offset: totalKeyOffset. Length: 4.
	// Check if in buffer.
	vLenStart := totalKeyOffset
	vLenEnd := vLenStart + 4

	var valLen uint32
	if int64(n) >= vLenEnd {
		valLen = binary.BigEndian.Uint32(buf[vLenStart:vLenEnd])
	} else {
		// Read 4 bytes
		vLenBuf := make([]byte, 4)
		if _, err := seg.ReadAtBuffer(vLenBuf, offset+vLenStart); err != nil {
			return Entry{}, err
		}
		valLen = binary.BigEndian.Uint32(vLenBuf)
	}

	// Value
	valStart := vLenEnd
	valEnd := valStart + int64(valLen)

	var val []byte
	if int64(n) >= valEnd {
		// Value in buffer.
		val = make([]byte, valLen)
		copy(val, buf[valStart:valEnd])
	} else {
		// Value outside.
		val = make([]byte, valLen)
		if _, err := seg.ReadAtBuffer(val, offset+valStart); err != nil {
			return Entry{}, err
		}
	}

	return Entry{
		Type:      entryType,
		TxID:      txID,
		Key:       key,
		Value:     val,
		ExpiresAt: expiresAt,
	}, nil
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

	if err := m.active.Close(); err != nil {
		return err
	}
	for _, s := range m.sealed {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) ActiveSegmentID() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active.ID()
}

func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
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
		header, err := seg.ReadAt(offset, 21)
		if err != nil {
			return err
		}

		typ := header[0]
		txID := binary.BigEndian.Uint64(header[1:])
		expiresAt := int64(binary.BigEndian.Uint64(header[9:]))
		keyLen := binary.BigEndian.Uint32(header[17:])

		key, err := seg.ReadAt(offset+21, int(keyLen))
		if err != nil {
			return err
		}

		vLenOffset := offset + 21 + int64(keyLen)
		vLenBuf, err := seg.ReadAt(vLenOffset, 4)
		if err != nil {
			return err
		}
		valLen := binary.BigEndian.Uint32(vLenBuf)

		val, err := seg.ReadAt(vLenOffset+4, int(valLen))
		if err != nil {
			return err
		}

		err = fn(Entry{
			Type:      typ,
			TxID:      txID,
			Key:       string(key),
			Value:     val,
			ExpiresAt: expiresAt,
		}, PackOffset(seg.ID(), offset))
		if err != nil {
			return err
		}

		offset = vLenOffset + 4 + int64(valLen)
	}
	return nil
}

func (m *Manager) SetMaxSegmentSize(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxSize = size
}

func (m *Manager) IterateActiveSegment(fn func(e Entry, offset int64) error) error {
	m.mu.RLock()
	active := m.active
	m.mu.RUnlock()

	return m.IterateSegment(active, fn)
}
