package wal

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

// Segment represents a single file in the segmented WAL.
type Segment struct {
	mu      sync.RWMutex
	id      uint64
	path    string
	file    *os.File
	size    int64
	maxSize int64
	closed  bool
	direct  bool
}

// NewSegment creates or opens a segment.
func NewSegment(id uint64, path string, maxSize int64) (*Segment, error) {
	flags := os.O_RDWR | os.O_APPEND | os.O_CREATE | syscall.O_DIRECT
	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		// Fallback for systems without O_DIRECT (e.g. some containers/FS)
		flags = os.O_RDWR | os.O_APPEND | os.O_CREATE
		f, err = os.OpenFile(path, flags, 0644)
		if err != nil {
			return nil, fmt.Errorf("segment: failed to open: %w", err)
		}
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Segment{
		id:      id,
		path:    path,
		file:    f,
		size:    stat.Size(),
		maxSize: maxSize,
		direct:  (flags & syscall.O_DIRECT) != 0,
	}, nil
}

// Write appends data to the segment.
func (s *Segment) Write(data []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, fmt.Errorf("segment: closed")
	}

	if s.size+int64(len(data)) > s.maxSize {
		return 0, ErrSegmentFull
	}

	n, err := s.file.Write(data)
	if err != nil {
		// If O_DIRECT is enabled and we get Invalid Argument (EINVAL),
		// it likely means the buffer is unaligned or FS doesn't support it strictly.
		// Fallback to buffered I/O.
		if s.direct && (os.IsPermission(err) || isInvalidArg(err)) {
			if err := s.disableDirectIO(); err != nil {
				return 0, fmt.Errorf("segment: fallback failed: %w", err)
			}
			// Retry write
			n, err = s.file.Write(data)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	offset := s.size
	s.size += int64(n)
	return offset, nil
}

func (s *Segment) disableDirectIO() error {
	s.file.Close()
	flags := os.O_RDWR | os.O_APPEND | os.O_CREATE
	f, err := os.OpenFile(s.path, flags, 0644)
	if err != nil {
		return err
	}
	s.file = f
	s.direct = false
	return nil
}

func isInvalidArg(err error) bool {
	if serr, ok := err.(syscall.Errno); ok {
		return serr == syscall.EINVAL
	}
	if perr, ok := err.(*os.PathError); ok {
		if serr, ok := perr.Err.(syscall.Errno); ok {
			return serr == syscall.EINVAL
		}
	}
	return false
}

// ReadAt reads data from a specific offset.
func (s *Segment) ReadAt(offset int64, size int) ([]byte, error) {
	for {
		s.mu.RLock()
		f := s.file
		direct := s.direct
		s.mu.RUnlock()

		if f == nil {
			s.mu.Lock()
			if s.file == nil {
				flags := os.O_RDONLY
				// Rely on OS page cache for read speed
				newFile, err := os.OpenFile(s.path, flags, 0644)
				if err != nil {
					s.mu.Unlock()
					return nil, err
				}
				s.file = newFile
			}
			s.mu.Unlock()
			continue
		}

		buf := make([]byte, size)
		if _, err := f.ReadAt(buf, offset); err != nil {
			if direct && isInvalidArg(err) {
				s.mu.Lock()
				if err := s.disableDirectIO(); err != nil {
					s.mu.Unlock()
					return nil, fmt.Errorf("segment: fallback read failed: %w", err)
				}
				s.mu.Unlock()
				continue
			}
			return nil, err
		}
		return buf, nil
	}
}

// ReadAtBuffer reads data into the provided buffer.
// It returns the number of bytes read and any error.
func (s *Segment) ReadAtBuffer(buf []byte, offset int64) (int, error) {
	s.mu.RLock()
	if s.file == nil {
		s.mu.RUnlock()
		s.mu.Lock()
		if s.file == nil {
			flags := os.O_RDONLY
			f, err := os.OpenFile(s.path, flags, 0644)
			if err != nil {
				s.mu.Unlock()
				return 0, err
			}
			s.file = f
		}
		s.mu.Unlock()
		s.mu.RLock()
	}
	defer s.mu.RUnlock()

	return s.file.ReadAt(buf, offset)
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}

func (s *Segment) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return nil
	}
	return s.file.Sync()
}

func (s *Segment) ID() uint64 {
	return s.id
}

func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}
