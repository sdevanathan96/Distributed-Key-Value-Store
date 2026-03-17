package lsm

import "bytes"

type SSTableIterator struct {
	sst     *SSTable
	offset  int64
	endOff  int64
	current Entry
	valid   bool
}

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
func (it *SSTableIterator) Entry() Entry {
	if it.Valid() {
		return it.current
	}
	return Entry{}
}
func (it *SSTableIterator) Valid() bool {
	return it.valid
}

type MergeIterator struct {
	iters []*SSTableIterator
}

func NewMergeIterator(iters []*SSTableIterator) *MergeIterator {
	return &MergeIterator{iters}
}

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
