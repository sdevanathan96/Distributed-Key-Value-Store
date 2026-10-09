package storage

import (
	"bytes"
	"errors"
)

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrWALClosed    = errors.New("wal is closed")
	ErrEngineClosed = errors.New("engine is closed")
)

type EntryType uint8

const (
	EntryPut EntryType = iota + 1
	EntryDelete
)

type KVPair struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Timestamp int64
}

// Less orders KVPairs by key, providing the comparison the MemTable btree uses.
func (kv KVPair) Less(than KVPair) bool {
	cmp := bytes.Compare(kv.Key, than.Key)
	return cmp < 0
}

type WALEntry struct {
	Key   []byte
	Value []byte
	Type  EntryType
	// Index is the WAL's own record number, used by the truncation watermark.
	Index uint64
	// Seq is the Raft log index of the write, 0 for an unsequenced write. It
	// occupies the record slot that held an always zero term, so older WAL
	// files decode with Seq 0.
	Seq uint64
}

type Command struct {
	Op    EntryType
	Key   []byte
	Value []byte
}
