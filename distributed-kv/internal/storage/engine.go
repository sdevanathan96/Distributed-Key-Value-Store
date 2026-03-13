package storage

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type Engine struct {
	mu            sync.RWMutex
	wal           *WAL
	memTable      *MemTable
	immutable     *MemTable
	config        StorageConfig
	closed        bool
	nextSSTableID atomic.Uint64
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
	engine := &Engine{
		config:    config,
		wal:       wal,
		memTable:  mem,
		immutable: nil,
		closed:    false,
	}
	engine.nextSSTableID.Store(0)
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
	val, found := e.memTable.Get(key)
	if found {
		return val, nil
	}
	if e.immutable != nil {
		val, found = e.immutable.Get(key)
		if found {
			return val, nil
		}
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
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	err := e.wal.Close()
	if err != nil {
		return fmt.Errorf("close wal: %w", err)
	}
	if e.memTable.Len() > 0 {
		e.triggerFlush()
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
	return e.nextSSTableID.Add(1)
}
