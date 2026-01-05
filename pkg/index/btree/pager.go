package btree

import (
	"fmt"
	"os"
	"sync"
)

const PageSize = 4096

type PageID uint64

type Pager struct {
	file *os.File
	mu   sync.RWMutex
	size int64
}

func NewPager(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Pager{
		file: f,
		size: stat.Size(),
	}, nil
}

func (p *Pager) ReadPage(id PageID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	offset := int64(id) * PageSize
	if offset+PageSize > p.size {
		return nil, fmt.Errorf("pager: read out of bounds")
	}

	buf := make([]byte, PageSize)
	if _, err := p.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *Pager) WritePage(id PageID, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(data) != PageSize {
		return fmt.Errorf("pager: invalid page size")
	}

	offset := int64(id) * PageSize
	if _, err := p.file.WriteAt(data, offset); err != nil {
		return err
	}

	if offset+PageSize > p.size {
		p.size = offset + PageSize
	}
	return nil
}

func (p *Pager) AllocatePage() PageID {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := PageID(p.size / PageSize)
	p.size += PageSize
	return id
}

func (p *Pager) Close() error {
	return p.file.Close()
}
