package storage

import (
	"distributed-kv/internal/storage/lsm"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (e *Engine) triggerFlush() {
	if e.immutable != nil {
		log.Printf("Flush skipped: previous flush still in progress")
		return
	}
	e.immutable = e.memTable
	e.memTable = NewMemTable(e.config.MemTableSize)
	go e.flushImmutable()
}

func (e *Engine) flushImmutable() {
	e.mu.RLock()
	im := e.immutable
	e.mu.RUnlock()
	if im == nil {
		return
	}
	entries := im.Entries()
	id := e.allocateSSTableID()
	filename := fmt.Sprintf("%06d.sst", id)
	dir := filepath.Join(e.config.SSTableDir, "L0")
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}
	path := filepath.Join(dir, filename)

	writer, err := lsm.NewSSTableWriter(path, uint(len(entries)))
	if err != nil {
		return
	}

	for _, kvPair := range entries {
		entry := lsm.Entry{
			Key:       kvPair.Key,
			Value:     kvPair.Value,
			Tombstone: kvPair.Tombstone,
			Timestamp: kvPair.Timestamp,
		}
		err := writer.WriteEntry(entry)
		if err != nil {
			return
		}
	}

	//meta, err := writer.Finish()
	_, err = writer.Finish()
	if err != nil {
		return
	}

	e.mu.Lock()
	e.immutable = nil
	e.mu.Unlock()

	log.Printf("Flushed %d entries to %s", len(entries), path)

	// e.lsm.AddSSTable(path, meta, 0)
}
