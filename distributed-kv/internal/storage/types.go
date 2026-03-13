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
	EntryPut EntryType = iota + 1 // start at 1, not 0
	EntryDelete
)

type KVPair struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Timestamp int64
}

func (kv KVPair) Less(than KVPair) bool {
	cmp := bytes.Compare(kv.Key, than.Key)
	return cmp < 0
}

type WALEntry struct {
	Key   []byte
	Value []byte
	Type  EntryType
	Index uint64
	Term  uint64
}

type Command struct {
	Op    EntryType
	Key   []byte
	Value []byte
}
