package lsm

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (l *LSMTree) compact(sourceLevel int) {
	if !l.compacting.CompareAndSwap(false, true) {
		return
	}
	defer l.compacting.Store(false)

	targetLevel := sourceLevel + 1
	if targetLevel >= MaxLevels {
		log.Printf("Cannot compact: target level %d exceeds max", targetLevel)
		return
	}

	l.mu.RLock()
	sourceSSTables := l.levels[sourceLevel].SSTables()
	l.mu.RUnlock()

	if sourceLevel == 0 && len(sourceSSTables) < L0CompactionThreshold {
		return
	}

	globalMin, globalMax := computeKeyRange(sourceSSTables)

	var iters []*SSTableIterator
	var estimatedEntries int64
	for _, sst := range sourceSSTables {
		estimatedEntries += sst.meta.EntryCount
		it, err := NewSSTableIterator(sst)
		if err != nil {
			log.Printf("compact: source iterator error: %v", err)
			return
		}
		iters = append(iters, it)
	}

	l.mu.Lock()
	targetSSTables := l.levels[targetLevel].SSTables()
	log.Printf("DEBUG compact: L%d has %d SSTables at snapshot time", targetLevel, len(targetSSTables))
	var overlapping []*SSTable
	for _, sst := range targetSSTables {
		if bytes.Compare(sst.meta.MinKey, globalMax) <= 0 &&
			bytes.Compare(sst.meta.MaxKey, globalMin) >= 0 {
			overlapping = append(overlapping, sst)
		}
	}
	log.Printf("DEBUG compact: source globalMin=%s globalMax=%s", string(globalMin), string(globalMax))
	for _, sst := range targetSSTables {
		log.Printf("DEBUG compact: L%d SSTable %s [%s .. %s]",
			targetLevel, sst.path, string(sst.meta.MinKey), string(sst.meta.MaxKey))
		log.Printf("DEBUG compact: overlap check: MinKey<=globalMax? %v  MaxKey>=globalMin? %v",
			bytes.Compare(sst.meta.MinKey, globalMax) <= 0,
			bytes.Compare(sst.meta.MaxKey, globalMin) >= 0)
	}
	log.Printf("DEBUG compact: found %d overlapping", len(overlapping))
	l.mu.Unlock()

	for _, sst := range overlapping {
		estimatedEntries += sst.meta.EntryCount
		it, err := NewSSTableIterator(sst)
		if err != nil {
			log.Printf("compact: target iterator error: %v", err)
			return
		}
		iters = append(iters, it)
	}

	merger := NewMergeIterator(iters)

	id := l.AllocateSSTableID()
	dir := filepath.Join(l.dir, fmt.Sprintf("L%d", targetLevel))
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("compact: mkdir error: %v", err)
		return
	}
	path := filepath.Join(dir, fmt.Sprintf("%06d.sst", id))

	writer, err := NewSSTableWriter(path, uint(estimatedEntries))
	if err != nil {
		log.Printf("compact: writer create error: %v", err)
		return
	}

	var count int64
	for {
		entry, ok := merger.Next()
		if !ok {
			break
		}
		if err := writer.WriteEntry(entry); err != nil {
			log.Printf("compact: write error: %v", err)
			return
		}
		count++
	}

	if _, err := writer.Finish(); err != nil {
		log.Printf("compact: finish error: %v", err)
		return
	}

	newSST, err := OpenSSTable(path, targetLevel)
	if err != nil {
		log.Printf("compact: open new sstable error: %v", err)
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.levels[targetLevel].AddSSTable(newSST)
	if err := l.manifest.AddSSTable(path, targetLevel); err != nil {
		log.Printf("compact: manifest add error: %v", err)
		return
	}

	for _, sst := range sourceSSTables {
		l.levels[sourceLevel].RemoveSSTable(sst.path)
		l.manifest.RemoveSSTable(sst.path)
		os.Remove(sst.path)
	}

	for _, sst := range overlapping {
		l.levels[targetLevel].RemoveSSTable(sst.path)
		l.manifest.RemoveSSTable(sst.path)
		os.Remove(sst.path)
	}

	log.Printf("Compacted %d L%d + %d L%d SSTables into 1 L%d SSTable (%d entries)",
		len(sourceSSTables), sourceLevel, len(overlapping), targetLevel, targetLevel, count)
}

func computeKeyRange(sstables []*SSTable) ([]byte, []byte) {
	if len(sstables) == 0 {
		return nil, nil
	}
	globalMin := sstables[0].meta.MinKey
	globalMax := sstables[0].meta.MaxKey
	for _, sst := range sstables[1:] {
		if bytes.Compare(sst.meta.MinKey, globalMin) < 0 {
			globalMin = sst.meta.MinKey
		}
		if bytes.Compare(sst.meta.MaxKey, globalMax) > 0 {
			globalMax = sst.meta.MaxKey
		}
	}
	return globalMin, globalMax
}
