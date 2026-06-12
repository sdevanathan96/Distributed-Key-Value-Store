package lsm

import "bytes"

type SSTableIterator struct {
	sst     *SSTable
	offset  int64
	endOff  int64
	current Entry
	valid   bool
}

// NewSSTableIterator returns an iterator positioned at the first entry of sst,
// or an invalid iterator if the table holds no data entries.
func NewSSTableIterator(sst *SSTable) (*SSTableIterator, error) {
	var valid bool
	var currentEntry = Entry{}
	var offset int64 = 0
	var endOff = sst.meta.IndexOffset
	entry, bytesRead, err := sst.readEntryAt(0)
	if err == nil {
		currentEntry = entry
		valid = true
	}
	if endOff == 0 {
		valid = false
	}
	offset += bytesRead
	return &SSTableIterator{
		sst,
		offset,
		endOff,
		currentEntry,
		valid,
	}, err

}

// Next advances to the following entry and reports whether one was read.
func (it *SSTableIterator) Next() bool {
	if !it.valid {
		return false
	}
	if it.offset >= it.endOff {
		it.valid = false
		return false
	}
	at, bytesRead, err := it.sst.readEntryAt(it.offset)
	if err != nil {
		it.valid = false
		return false
	}
	it.current = at
	it.offset += bytesRead
	return true
}

// Entry returns the entry at the current position, or the zero Entry if the
// iterator is exhausted.
func (it *SSTableIterator) Entry() Entry {
	if it.Valid() {
		return it.current
	}
	return Entry{}
}

// Valid reports whether the iterator is positioned at a readable entry.
func (it *SSTableIterator) Valid() bool {
	return it.valid
}

type MergeIterator struct {
	iters []*SSTableIterator
}

// NewMergeIterator returns an iterator that merges the given per table
// iterators into a single ascending key stream.
func NewMergeIterator(iters []*SSTableIterator) *MergeIterator {
	return &MergeIterator{iters}
}

// Next returns the next entry in merged key order, collapsing duplicate keys to
// the newest version by timestamp. ok is false once all inputs are exhausted.
func (m *MergeIterator) Next() (Entry, bool) {
	var bestIdx = -1
	var bestEntry Entry

	for i, it := range m.iters {
		if !it.Valid() {
			continue
		}
		e := it.Entry()
		if bestIdx == -1 {
			bestIdx = i
			bestEntry = e
			continue
		}
		cmp := bytes.Compare(e.Key, bestEntry.Key)
		if cmp < 0 {
			bestIdx = i
			bestEntry = e
		} else if cmp == 0 && e.Timestamp > bestEntry.Timestamp {
			bestIdx = i
			bestEntry = e
		}
	}

	if bestIdx == -1 {
		return Entry{}, false
	}

	for _, it := range m.iters {
		if !it.Valid() {
			continue
		}
		if bytes.Equal(it.Entry().Key, bestEntry.Key) {
			it.Next()
		}
	}

	return bestEntry, true
}
