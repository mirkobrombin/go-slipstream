package btree

import (
	"bytes"
	"encoding/binary"
	"sort"
)

const (
	NodeTypeInternal = 0
	NodeTypeLeaf     = 1
)

type Item struct {
	Key   []byte
	Value int64
}

type Node struct {
	ID       PageID
	Type     uint8
	Items    []Item
	Children []PageID
	Next     PageID
	dirty    bool
}

func (n *Node) Serialize(buf []byte) {
	// Clear buffer
	for i := range buf {
		buf[i] = 0
	}

	buf[0] = n.Type
	binary.BigEndian.PutUint16(buf[1:], uint16(len(n.Items)))
	binary.BigEndian.PutUint64(buf[3:], uint64(n.Next))

	// Heap starts at end of page
	heapPos := len(buf)
	offsetPos := 13

	if n.Type == NodeTypeInternal {
		if len(n.Children) > 0 {
			binary.BigEndian.PutUint64(buf[3:], uint64(n.Children[0]))
		}
	} else {
		// Leaf: Store Next Ptr
		binary.BigEndian.PutUint64(buf[3:], uint64(n.Next))
	}

	for i, item := range n.Items {
		kLen := len(item.Key)
		itemSize := 2 + kLen + 8
		heapPos -= itemSize

		// Write Offset
		binary.BigEndian.PutUint16(buf[offsetPos:], uint16(heapPos))
		offsetPos += 2

		// Write Data
		binary.BigEndian.PutUint16(buf[heapPos:], uint16(kLen))
		copy(buf[heapPos+2:], item.Key)

		valOrChild := item.Value
		if n.Type == NodeTypeInternal {
			if i+1 < len(n.Children) {
				valOrChild = int64(n.Children[i+1])
			} else {
				valOrChild = 0
			}
		}
		binary.BigEndian.PutUint64(buf[heapPos+2+kLen:], uint64(valOrChild))
	}
}

func DeserializeNode(id PageID, buf []byte) *Node {
	n := &Node{ID: id}
	n.Type = buf[0]
	numItems := binary.BigEndian.Uint16(buf[1:])

	n.Items = make([]Item, numItems)

	if n.Type == NodeTypeInternal {
		n.Children = make([]PageID, numItems+1)
		n.Children[0] = PageID(binary.BigEndian.Uint64(buf[3:]))
	} else {
		n.Next = PageID(binary.BigEndian.Uint64(buf[3:]))
	}

	offsetPos := 13
	for i := 0; i < int(numItems); i++ {
		off := binary.BigEndian.Uint16(buf[offsetPos:])
		offsetPos += 2

		kLen := binary.BigEndian.Uint16(buf[off:])
		key := make([]byte, kLen)
		copy(key, buf[off+2:int(off)+2+int(kLen)])
		valOrChild := int64(binary.BigEndian.Uint64(buf[off+2+uint16(kLen):]))

		n.Items[i] = Item{Key: key, Value: valOrChild}

		if n.Type == NodeTypeInternal {
			n.Children[i+1] = PageID(valOrChild)
		}
	}

	return n
}

func (n *Node) Insert(key []byte, value int64, child PageID) {
	// Find position
	idx := sort.Search(len(n.Items), func(i int) bool {
		return bytes.Compare(n.Items[i].Key, key) >= 0
	})

	// If exists, update
	if idx < len(n.Items) && bytes.Equal(n.Items[idx].Key, key) {
		n.Items[idx].Value = value
		return
	}

	// Insert
	n.Items = append(n.Items, Item{})
	copy(n.Items[idx+1:], n.Items[idx:])
	n.Items[idx] = Item{Key: key, Value: value}

	if n.Type == NodeTypeInternal {
		n.Children = append(n.Children, 0)
		copy(n.Children[idx+2:], n.Children[idx+1:])
		n.Children[idx+1] = child
	}
	n.dirty = true
}

func (n *Node) IsFull() bool {
	size := 13 + len(n.Items)*2
	for _, it := range n.Items {
		size += 2 + len(it.Key) + 8
	}
	return size > PageSize
}
