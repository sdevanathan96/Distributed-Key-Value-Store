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
}

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
		config:    config,
		wal:       wal,
		memTable:  mem,
		immutable: nil,
		closed:    false,
		lsm:       lsmTree,
	}
	return engine, nil
}

func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	_, err := e.wal.Append(WALEntry{
		Type:  EntryPut,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	e.memTable.Put(key, value)
	if e.memTable.ShouldFlush() {
		log.Printf("MemTable full (%d bytes), flushing..", e.memTable.size)
		e.triggerFlush()
	}
	return nil
}

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

func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	_, err := e.wal.Append(WALEntry{
		Type:  EntryDelete,
		Key:   key,
		Value: nil,
	})
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	e.memTable.Delete(key)
	if e.memTable.ShouldFlush() {
		log.Printf("MemTable full (%d bytes), flush needed", e.memTable.size)
		e.triggerFlush()
	}
	return nil
}

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

func (e *Engine) allocateSSTableID() uint64 {
	return e.lsm.AllocateSSTableID()
}

func (e *Engine) WaitForBackground() {
	e.wg.Wait()
	e.lsm.WaitForBackground()
}
