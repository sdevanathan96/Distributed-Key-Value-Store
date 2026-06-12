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

// NewMemTable returns an empty MemTable that reports ShouldFlush once its
// estimated size reaches maxSize.
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

// estimateSize approximates the in memory footprint of a key value pair,
// including a fixed per entry overhead.
func estimateSize(key, value []byte) int64 {
	return int64(len(key)) + int64(len(value)) + 41
}

// Put inserts or replaces the value for key and updates the tracked size.
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

// Get returns the value for key. It reports found as false if the key is absent
// or holds a tombstone.
func (m *MemTable) Get(key []byte) (value []byte, found bool) {
	result, exists := m.tree.Get(KVPair{Key: key})
	if exists && !result.Tombstone {
		return result.Value, exists
	}
	return nil, false
}

// GetT returns the value for key along with whether it was found and whether
// the entry is a tombstone, letting callers stop a multi level lookup at a
// deletion.
func (m *MemTable) GetT(key []byte) (value []byte, found bool, tombstone bool) {
	result, exists := m.tree.Get(KVPair{Key: key})
	if exists && !result.Tombstone {
		return result.Value, exists, false
	} else if exists && result.Tombstone {
		return result.Value, exists, true
	}
	return nil, false, false
}

// Delete records a tombstone for key so it shadows any value in lower levels.
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

// ShouldFlush reports whether the table has reached its size limit and should
// be flushed to an SSTable.
func (m *MemTable) ShouldFlush() bool {
	return m.size >= m.maxSize
}

// Entries returns all key value pairs, including tombstones, in ascending key
// order.
func (m *MemTable) Entries() []KVPair {
	items := make([]KVPair, 0, m.count)
	m.tree.Ascend(KVPair{}, func(item KVPair) bool {
		items = append(items, item)
		return true
	})
	return items
}

// Len returns the number of entries in the table, counting tombstones.
func (m *MemTable) Len() int {
	return m.tree.Len()
}
