package storage

import (
	"distributed-kv/internal/storage/lsm"
	"fmt"
	"log"
	"sync"
)

type Engine struct {
	mu        sync.RWMutex
	wg        sync.WaitGroup
	wal       *WAL
	memTable  *MemTable
	immutable *MemTable
	config    StorageConfig
	closed    bool
	lsm       *lsm.LSMTree
	// appliedIndex is the highest seq written, recovered from the WAL on open.
	appliedIndex uint64
}

// NewEngine opens the storage engine at the configured paths, replaying the WAL
// into a fresh MemTable and opening the existing LSM tree.
func NewEngine(config StorageConfig) (*Engine, error) {
	err := config.EnsureDirs()
	if err != nil {
		return nil, fmt.Errorf("create dirs: %w", err)
	}
	wal, err := NewWAL(config)
	if err != nil {
		return nil, fmt.Errorf("create wal: %w", err)
	}
	mem := NewMemTable(config.MemTableSize)

	entries, err := wal.Recover()
	if err != nil {
		return nil, fmt.Errorf("recover wal: %w", err)
	}
	for _, entry := range entries {
		switch entry.Type {
		case EntryPut:
			mem.Put(entry.Key, entry.Value)
		case EntryDelete:
			mem.Delete(entry.Key)
		}
	}
	log.Printf("Engine recovered %d entries from WAL", len(entries))
	lsmTree, err := lsm.NewLSMTree(config.SSTableDir, config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create lsm: %w", err)
	}
	engine := &Engine{
		config:       config,
		wal:          wal,
		memTable:     mem,
		immutable:    nil,
		closed:       false,
		lsm:          lsmTree,
		appliedIndex: wal.MaxSeq(),
	}
	return engine, nil
}

// Put durably records key and value: it appends to the WAL, applies the write
// to the MemTable, and triggers a background flush when the MemTable is full.
// seq is the write's Raft log index (0 for an unsequenced write); it is stored
// in the same WAL record as the write, so AppliedIndex can never run ahead of
// or behind the data it describes.
func (e *Engine) Put(key, value []byte, seq uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	_, err := e.wal.Append(WALEntry{
		Type:  EntryPut,
		Key:   key,
		Value: value,
		Seq:   seq,
	})
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	e.memTable.Put(key, value)
	e.advanceApplied(seq)
	if e.memTable.ShouldFlush() {
		log.Printf("MemTable full (%d bytes), flushing..", e.memTable.size)
		e.triggerFlush()
	}
	return nil
}

// Get returns the value for key, searching the active MemTable, the immutable
// MemTable being flushed, and then the LSM tree in that order. A tombstone
// encountered along the way yields ErrKeyNotFound.
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrEngineClosed
	}
	val, found, tombstone := e.memTable.GetT(key)
	if found {
		if tombstone {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}
	if e.immutable != nil {
		val, found, tombstone = e.immutable.GetT(key)
		if found {
			if tombstone {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}
	entry, found, err := e.lsm.Get(key)
	if err != nil {
		return nil, fmt.Errorf("lsm get: %w", err)
	}
	if found {
		if entry.Tombstone {
			return nil, ErrKeyNotFound
		}
		return entry.Value, nil
	}
	return nil, ErrKeyNotFound
}

// Delete durably records a tombstone for key through the same WAL then MemTable
// path as Put, with the same seq semantics.
func (e *Engine) Delete(key []byte, seq uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	_, err := e.wal.Append(WALEntry{
		Type:  EntryDelete,
		Key:   key,
		Value: nil,
		Seq:   seq,
	})
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	e.memTable.Delete(key)
	e.advanceApplied(seq)
	if e.memTable.ShouldFlush() {
		log.Printf("MemTable full (%d bytes), flush needed", e.memTable.size)
		e.triggerFlush()
	}
	return nil
}

// AppliedIndex returns the highest seq durably written, including writes
// recovered from the WAL. Raft resumes applying after it on restart, so a
// restart never replays old writes over newer state.
//
// It is recovered from the WAL, which holds every record because nothing
// deletes segment files. Deleting segments after a flush (snapshots, Phase 2
// Stage 3) must first record the flushed seq somewhere that survives.
func (e *Engine) AppliedIndex() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.appliedIndex
}

// advanceApplied raises appliedIndex to seq. The caller must hold e.mu.
func (e *Engine) advanceApplied(seq uint64) {
	if seq > e.appliedIndex {
		e.appliedIndex = seq
	}
}

// Close marks the engine closed, waits for background flushes to finish, and
// closes the LSM tree and WAL.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	e.wg.Wait()
	err := e.lsm.Close()
	if err != nil {
		return fmt.Errorf("lsm close: %w", err)
	}
	err = e.wal.Close()
	if err != nil {
		return fmt.Errorf("close wal: %w", err)
	}
	return nil
}

// GetSnapshot returns the live, non tombstone contents of the active MemTable
// as a map. It does not include data already flushed to SSTables.
func (e *Engine) GetSnapshot() map[string][]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	entries := e.memTable.Entries()
	result := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		if !entry.Tombstone {
			key := string(entry.Key)
			result[key] = entry.Value
		}
	}
	return result
}

// allocateSSTableID returns the next unique SSTable identifier from the LSM
// manifest.
func (e *Engine) allocateSSTableID() uint64 {
	return e.lsm.AllocateSSTableID()
}

// WaitForBackground blocks until all background flush and compaction goroutines
// have finished. Intended for tests.
func (e *Engine) WaitForBackground() {
	e.wg.Wait()
	e.lsm.WaitForBackground()
}
