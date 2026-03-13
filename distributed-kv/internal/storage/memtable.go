package storage

import (
	"time"

	"github.com/tidwall/btree"
)

type MemTable struct {
	tree    *btree.BTreeG[KVPair]
	size    int64
	count   int64
	maxSize int64
}

func NewMemTable(maxSize int64) *MemTable {
	tree := btree.NewBTreeG[KVPair](func(a, b KVPair) bool {
		return a.Less(b)
	})
	return &MemTable{
		tree:    tree,
		maxSize: maxSize,
		size:    0,
		count:   0,
	}
}

func estimateSize(key, value []byte) int64 {
	return int64(len(key)) + int64(len(value)) + 41
}

func (m *MemTable) Put(key, value []byte) {
	pair := KVPair{
		Key:       key,
		Value:     value,
		Tombstone: false,
		Timestamp: time.Now().UnixNano(),
	}
	prev, replaced := m.tree.Set(pair)
	if replaced {
		m.size -= estimateSize(prev.Key, prev.Value)
	} else {
		m.count++
	}
	m.size += estimateSize(key, value)
}

func (m *MemTable) Get(key []byte) (value []byte, found bool) {
	result, exists := m.tree.Get(KVPair{Key: key})
	if exists && !result.Tombstone {
		return result.Value, exists
	}
	return nil, false
}

func (m *MemTable) GetT(key []byte) (value []byte, found bool, tombstone bool) {
	result, exists := m.tree.Get(KVPair{Key: key})
	if exists && !result.Tombstone {
		return result.Value, exists, false
	} else if exists && result.Tombstone {
		return result.Value, exists, true
	}
	return nil, false, false
}

func (m *MemTable) Delete(key []byte) {
	pair := KVPair{
		Key:       key,
		Tombstone: true,
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
	}
	prev, replaced := m.tree.Set(pair)
	if replaced {
		m.size -= estimateSize(prev.Key, prev.Value)
	} else {
		m.count++
	}
	m.size += estimateSize(key, nil)
}
func (m *MemTable) ShouldFlush() bool {
	return m.size >= m.maxSize
}

func (m *MemTable) Entries() []KVPair {
	items := make([]KVPair, 0, m.count)
	m.tree.Ascend(KVPair{}, func(item KVPair) bool {
		items = append(items, item)
		return true
	})
	return items
}
func (m *MemTable) Len() int {
	return m.tree.Len()
}
