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
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.flushImmutable()
	}()
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

	_, err = writer.Finish()
	if err != nil {
		return
	}

	sst, err := lsm.OpenSSTable(path, 0)
	if err != nil {
		log.Printf("open sstable error: %v", err)
		return
	}

	err = e.lsm.AddSSTable(sst, 0)
	if err != nil {
		log.Printf("add sstable error: %v", err)
		return
	}

	e.mu.Lock()
	e.immutable = nil
	e.mu.Unlock()

	log.Printf("Flushed %d entries to %s", len(entries), path)
}
