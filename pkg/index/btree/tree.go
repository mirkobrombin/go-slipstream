package btree

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
)

type BTree struct {
	mu    sync.RWMutex
	pager *Pager
	root  PageID
}

func New(path string) (*BTree, error) {
	pager, err := NewPager(path)
	if err != nil {
		return nil, err
	}

	t := &BTree{pager: pager}
	if pager.size == 0 {
		// Initialize
		// Page 0: Meta (Root = 1)
		// Page 1: Root (Leaf)
		if err := t.init(); err != nil {
			return nil, err
		}
	} else {
		// Read Meta
		buf, err := pager.ReadPage(0)
		if err != nil {
			return nil, err
		}
		t.root = PageID(binary.BigEndian.Uint64(buf))
	}

	return t, nil
}

func (t *BTree) init() error {
	meta := make([]byte, PageSize)
	binary.BigEndian.PutUint64(meta, 1) // Root is Page 1
	if err := t.pager.WritePage(0, meta); err != nil {
		return err
	}

	root := &Node{ID: 1, Type: NodeTypeLeaf}
	buf := make([]byte, PageSize)
	root.Serialize(buf)
	if err := t.pager.WritePage(1, buf); err != nil {
		return err
	}

	t.root = 1
	return nil
}

func (t *BTree) Get(key []byte) (int64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	currID := t.root
	for {
		buf, err := t.pager.ReadPage(currID)
		if err != nil {
			return 0, false
		}
		node := DeserializeNode(currID, buf)

		idx := sort.Search(len(node.Items), func(i int) bool {
			return bytes.Compare(node.Items[i].Key, key) >= 0
		})

		if node.Type == NodeTypeLeaf {
			if idx < len(node.Items) && bytes.Equal(node.Items[idx].Key, key) {
				return node.Items[idx].Value, true
			}
			return 0, false
		} else {
			// Internal
			if idx < len(node.Items) && bytes.Equal(node.Items[idx].Key, key) {
				currID = node.Children[idx+1]
			} else {
				currID = node.Children[idx]
			}
		}
	}
}

func (t *BTree) Put(key []byte, value int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	newChild, splitKey, err := t.insertRecursive(t.root, key, value)
	if err != nil {
		return err
	}

	if newChild != 0 {
		newRootID := t.pager.AllocatePage()
		newRoot := &Node{
			ID:       newRootID,
			Type:     NodeTypeInternal,
			Items:    []Item{{Key: splitKey}},
			Children: []PageID{t.root, newChild},
		}

		if err := t.writeNode(newRoot); err != nil {
			return err
		}

		t.root = newRootID
		meta := make([]byte, PageSize)
		binary.BigEndian.PutUint64(meta, uint64(t.root))
		if err := t.pager.WritePage(0, meta); err != nil {
			return err
		}
	}
	return nil
}

func (t *BTree) insertRecursive(pageID PageID, key []byte, value int64) (PageID, []byte, error) { // returns newSibling, medianKey
	buf, err := t.pager.ReadPage(pageID)
	if err != nil {
		return 0, nil, err
	}
	node := DeserializeNode(pageID, buf)

	var newChild PageID
	var splitKey []byte

	idx := sort.Search(len(node.Items), func(i int) bool {
		return bytes.Compare(node.Items[i].Key, key) >= 0
	})

	if node.Type == NodeTypeLeaf {
		node.Insert(key, value, 0)
	} else {
		childIdx := idx
		if idx < len(node.Items) && bytes.Equal(node.Items[idx].Key, key) {
			childIdx = idx + 1
		}

		childID := node.Children[childIdx]
		newChild, splitKey, err = t.insertRecursive(childID, key, value)
		if err != nil {
			return 0, nil, err
		}

		if newChild != 0 {
			node.Insert(splitKey, 0, newChild)
		}
	}

	if node.IsFull() {
		return t.split(node)
	}

	// Just write back
	// Optimization: check dirty
	if err := t.writeNode(node); err != nil {
		return 0, nil, err
	}
	return 0, nil, nil
}

func (t *BTree) split(node *Node) (PageID, []byte, error) {
	// Split into two nodes.
	mid := len(node.Items) / 2
	medianItem := node.Items[mid]

	siblingID := t.pager.AllocatePage()
	sibling := &Node{ID: siblingID, Type: node.Type}

	if node.Type == NodeTypeLeaf {
		sibling.Items = make([]Item, len(node.Items[mid:]))
		copy(sibling.Items, node.Items[mid:])
		node.Items = node.Items[:mid]

		// Update Next pointers
		sibling.Next = node.Next
		node.Next = siblingID

		if err := t.writeNode(sibling); err != nil {
			return 0, nil, err
		}
		if err := t.writeNode(node); err != nil {
			return 0, nil, err
		}

		// Key to promote: Sibling's first key
		return siblingID, sibling.Items[0].Key, nil
	} else {
		sibling.Items = make([]Item, len(node.Items[mid+1:]))
		copy(sibling.Items, node.Items[mid+1:])

		sibling.Children = make([]PageID, len(node.Children[mid+1:]))
		copy(sibling.Children, node.Children[mid+1:])

		node.Items = node.Items[:mid]
		node.Children = node.Children[:mid+1]

		if err := t.writeNode(sibling); err != nil {
			return 0, nil, err
		}
		if err := t.writeNode(node); err != nil {
			return 0, nil, err
		}

		return siblingID, medianItem.Key, nil
	}
}

func (t *BTree) writeNode(n *Node) error {
	buf := make([]byte, PageSize)
	n.Serialize(buf)
	return t.pager.WritePage(n.ID, buf)
}

func (t *BTree) Close() error {
	return t.pager.Close()
}
