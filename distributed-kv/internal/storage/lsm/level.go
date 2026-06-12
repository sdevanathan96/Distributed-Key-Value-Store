package lsm

import (
	"bytes"
	"sort"
	"sync"
)

type Level struct {
	mu       sync.RWMutex
	num      int
	sstables []*SSTable
}

// NewLevel returns an empty level numbered num.
func NewLevel(num int) *Level {
	return &Level{num: num, sstables: make([]*SSTable, 0)}
}

// AddSSTable inserts sst into the level. L0 tables are appended in arrival
// order; deeper levels keep tables sorted and non overlapping by key range.
func (l *Level) AddSSTable(sst *SSTable) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.num == 0 {
		l.sstables = append(l.sstables, sst)
	} else {
		idx := sort.Search(len(l.sstables), func(i int) bool {
			return bytes.Compare(l.sstables[i].meta.MinKey, sst.meta.MinKey) > 0
		})
		l.sstables = append(l.sstables, nil)
		copy(l.sstables[idx+1:], l.sstables[idx:])
		l.sstables[idx] = sst
	}
}

// RemoveSSTable closes and removes the table at path, reporting whether it was
// found.
func (l *Level) RemoveSSTable(path string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, sst := range l.sstables {
		if sst.path == path {
			err := sst.Close()
			if err != nil {
				return false
			}
			l.sstables = append(l.sstables[:i], l.sstables[i+1:]...)
			return true
		}
	}
	return false
}

// Get returns the entry for key from this level. L0 is scanned newest first
// since its tables may overlap; deeper levels binary search the one table whose
// key range covers key.
func (l *Level) Get(key []byte) (Entry, bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.num == 0 {
		for i := len(l.sstables) - 1; i >= 0; i-- {
			sst := l.sstables[i]
			entry, ok, err := sst.Get(key)
			if err != nil {
				return Entry{}, false, err
			}
			if ok {
				return entry, ok, err
			}
		}
		return Entry{}, false, nil
	} else {
		idx := sort.Search(len(l.sstables), func(i int) bool {
			return bytes.Compare(l.sstables[i].meta.MinKey, key) > 0
		}) - 1
		if idx >= 0 && bytes.Compare(key, l.sstables[idx].meta.MaxKey) <= 0 {
			return l.sstables[idx].Get(key)
		}
		return Entry{}, false, nil
	}
}

// SSTableCount returns the number of tables in the level.
func (l *Level) SSTableCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.sstables)
}

// SSTables returns a copy of the level's table slice, safe to iterate without
// holding the level lock.
func (l *Level) SSTables() []*SSTable {
	l.mu.RLock()
	defer l.mu.RUnlock()
	copied := make([]*SSTable, len(l.sstables))
	copy(copied, l.sstables)
	return copied
}
