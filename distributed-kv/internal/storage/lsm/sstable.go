package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
)

// ── CONSTANTS ─────────────────────────────────────────────────────────────

const (
	MagicNumber         uint32 = 0x53535401
	SparseIndexInterval        = 16
	BloomFPRate                = 0.01
)

type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Timestamp int64
}

type IndexEntry struct {
	Key    []byte
	Offset int64
}

type SSTableMeta struct {
	IndexOffset int64
	IndexLen    int64
	BloomOffset int64
	BloomLen    int64
	EntryCount  int64
	MinKey      []byte
	MaxKey      []byte
}

type SSTable struct {
	mu    sync.RWMutex
	file  *os.File
	path  string
	meta  SSTableMeta
	index []IndexEntry
	bloom *BloomFilter
	level int
}

type SSTableWriter struct {
	file       *os.File
	path       string
	index      []IndexEntry
	bloom      *BloomFilter
	offset     int64
	entryCount int64
	minKey     []byte
	maxKey     []byte
}

// YOUR CODE HERE: NewSSTableWriter function
func NewSSTableWriter(path string, estimatedEntries uint) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("error creating sstable file: %v", err)
	}
	filter := NewBloomFilter(estimatedEntries, BloomFPRate)
	return &SSTableWriter{
		file:       file,
		path:       path,
		index:      nil,
		bloom:      filter,
		offset:     0,
		entryCount: 0,
		minKey:     nil,
		maxKey:     nil,
	}, nil
}

func (w *SSTableWriter) WriteEntry(entry Entry) error {
	entryOffset := w.offset
	size := 2 + len(entry.Key) + 4 + len(entry.Value) + 1 + 8
	buf := make([]byte, size)
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(entry.Key)))
	pos += 2
	copy(buf[pos:], entry.Key)
	pos += len(entry.Key)
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(entry.Value)))
	pos += 4
	copy(buf[pos:], entry.Value)
	pos += len(entry.Value)
	if entry.Tombstone {
		buf[pos] = 1
	} else {
		buf[pos] = 0
	}
	pos += 1
	binary.BigEndian.PutUint64(buf[pos:], uint64(entry.Timestamp))
	n, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("error writing entry: %v", err)
	}
	w.offset += int64(n)
	w.bloom.Add(entry.Key)
	if w.entryCount%SparseIndexInterval == 0 {
		keyCpy := copyBytes(entry.Key)
		w.index = append(w.index, IndexEntry{
			Key:    keyCpy,
			Offset: entryOffset,
		})
	}
	if w.entryCount == 0 {
		w.minKey = copyBytes(entry.Key)
	}
	w.maxKey = copyBytes(entry.Key)
	w.entryCount++
	return nil
}

func (w *SSTableWriter) Finish() (*SSTableMeta, error) {
	indexOffset := w.offset
	for _, indexEntry := range w.index {
		size := 2 + len(indexEntry.Key) + 8
		buf := make([]byte, size)
		pos := 0
		binary.BigEndian.PutUint16(buf[pos:], uint16(len(indexEntry.Key)))
		pos += 2
		copy(buf[pos:pos+len(indexEntry.Key)], indexEntry.Key)
		pos += len(indexEntry.Key)
		binary.BigEndian.PutUint64(buf[pos:], uint64(indexEntry.Offset))
		n, err := w.file.Write(buf)
		if err != nil {
			return nil, fmt.Errorf("error writing entry: %v", err)
		}
		w.offset += int64(n)
	}
	indexLen := w.offset - indexOffset
	bloomOffset := w.offset
	bloomData, err := w.bloom.Serialize()
	if err != nil {
		return nil, fmt.Errorf("error serializing bloom data: %v", err)
	}
	n, err := w.file.Write(bloomData)
	if err != nil {
		return nil, fmt.Errorf("error writing bloom data: %v", err)
	}
	w.offset += int64(n)
	bloomLen := w.offset - bloomOffset
	footNoMagicSize := 8*5 + 2 + len(w.minKey) + 2 + len(w.maxKey)
	footerSize := footNoMagicSize + 4 + 4
	footer := make([]byte, footerSize)
	footPos := 0
	binary.BigEndian.PutUint64(footer[footPos:footPos+8], uint64(indexOffset))
	footPos += 8
	binary.BigEndian.PutUint64(footer[footPos:footPos+8], uint64(indexLen))
	footPos += 8
	binary.BigEndian.PutUint64(footer[footPos:footPos+8], uint64(bloomOffset))
	footPos += 8
	binary.BigEndian.PutUint64(footer[footPos:footPos+8], uint64(bloomLen))
	footPos += 8
	binary.BigEndian.PutUint64(footer[footPos:footPos+8], uint64(w.entryCount))
	footPos += 8
	binary.BigEndian.PutUint16(footer[footPos:footPos+2], uint16(len(w.minKey)))
	footPos += 2
	copy(footer[footPos:footPos+len(w.minKey)], w.minKey)
	footPos += len(w.minKey)
	binary.BigEndian.PutUint16(footer[footPos:footPos+2], uint16(len(w.maxKey)))
	footPos += 2
	copy(footer[footPos:footPos+len(w.maxKey)], w.maxKey)
	footPos += len(w.maxKey)
	binary.BigEndian.PutUint32(footer[footPos:footPos+4], uint32(footNoMagicSize))
	footPos += 4
	binary.BigEndian.PutUint32(footer[footPos:], MagicNumber)
	n, err = w.file.Write(footer)
	if err != nil {
		return nil, fmt.Errorf("error writing footer: %v", err)
	}
	w.offset += int64(n)
	err = w.file.Sync()
	if err != nil {
		return nil, fmt.Errorf("error syncing sstable file: %v", err)
	}
	err = w.file.Close()
	if err != nil {
		return nil, fmt.Errorf("error closing sstable file: %v", err)
	}
	return &SSTableMeta{
		IndexOffset: indexOffset,
		IndexLen:    indexLen,
		BloomOffset: bloomOffset,
		BloomLen:    bloomLen,
		EntryCount:  w.entryCount,
		MinKey:      w.minKey,
		MaxKey:      w.maxKey,
	}, nil
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func OpenSSTable(path string, level int) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening sstable file: %v", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error stat on sstable file: %v", err)
	}
	size := info.Size()
	buf := make([]byte, 8)
	_, err = file.ReadAt(buf, size-8)
	if err != nil {
		return nil, fmt.Errorf("error reading sstable file for extracting footer size and magic num: %v", err)
	}
	num := binary.BigEndian.Uint32(buf[4:])
	if num != MagicNumber {
		return nil, fmt.Errorf("invalid sstable file magic number: %d", num)
	}
	footerNoMagicSize := binary.BigEndian.Uint32(buf[0:4])
	footer := make([]byte, footerNoMagicSize)
	_, err = file.ReadAt(footer, size-int64(footerNoMagicSize)-8)
	if err != nil {
		return nil, fmt.Errorf("error reading footer in sstable file: %v", err)
	}
	footPos := 0
	indexOffset := binary.BigEndian.Uint64(footer[footPos : footPos+8])
	footPos += 8
	indexLen := binary.BigEndian.Uint64(footer[footPos : footPos+8])
	footPos += 8
	bloomOffset := binary.BigEndian.Uint64(footer[footPos : footPos+8])
	footPos += 8
	bloomLen := binary.BigEndian.Uint64(footer[footPos : footPos+8])
	footPos += 8
	entryCount := binary.BigEndian.Uint64(footer[footPos : footPos+8])
	footPos += 8
	minKeyLen := binary.BigEndian.Uint16(footer[footPos : footPos+2])
	footPos += 2
	minKey := make([]byte, minKeyLen)
	copy(minKey, footer[footPos:footPos+int(minKeyLen)])
	footPos += int(minKeyLen)
	maxKeyLen := binary.BigEndian.Uint16(footer[footPos : footPos+2])
	footPos += 2
	maxKey := make([]byte, maxKeyLen)
	copy(maxKey, footer[footPos:footPos+int(maxKeyLen)])
	footPos += int(maxKeyLen)
	index := make([]byte, indexLen)
	_, err = file.ReadAt(index, int64(indexOffset))
	if err != nil {
		return nil, fmt.Errorf("error reading sstable index: %v", err)
	}
	startIndexPos := uint64(0)
	var indices []IndexEntry
	for startIndexPos < indexLen {
		keyLen := binary.BigEndian.Uint16(index[startIndexPos : startIndexPos+2])
		startIndexPos += 2
		key := make([]byte, keyLen)
		copy(key, index[startIndexPos:startIndexPos+uint64(keyLen)])
		startIndexPos += uint64(keyLen)
		offset := binary.BigEndian.Uint64(index[startIndexPos : startIndexPos+8])
		startIndexPos += 8
		indices = append(indices, IndexEntry{
			Key:    key,
			Offset: int64(offset),
		})
	}
	meta := SSTableMeta{
		IndexOffset: int64(indexOffset),
		IndexLen:    int64(indexLen),
		BloomOffset: int64(bloomOffset),
		BloomLen:    int64(bloomLen),
		EntryCount:  int64(entryCount),
		MinKey:      minKey,
		MaxKey:      maxKey,
	}
	bloomBytes := make([]byte, bloomLen)
	_, err = file.ReadAt(bloomBytes, int64(bloomOffset))
	if err != nil {
		return nil, fmt.Errorf("error reading bloom filter bytes in sstable file: %v", err)
	}
	bloom, err := DeserializeBloom(bloomBytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing bloom filter bytes from sstable file: %v", err)
	}
	return &SSTable{
		file:  file,
		path:  path,
		meta:  meta,
		index: indices,
		bloom: bloom,
		level: level,
	}, nil
}

func (s *SSTable) Get(key []byte) (Entry, bool, error) {
	if bytes.Compare(key, s.meta.MinKey) < 0 {
		return Entry{}, false, nil
	}
	if bytes.Compare(key, s.meta.MaxKey) > 0 {
		return Entry{}, false, nil
	}
	if !s.bloom.MayContain(key) {
		return Entry{}, false, nil
	}
	idx := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].Key, key) > 0
	}) - 1
	if idx < 0 {
		idx = 0
	}
	startOffset := s.index[idx].Offset
	var endOffset int64
	if idx+1 < len(s.index) {
		endOffset = s.index[idx+1].Offset
	} else {
		endOffset = s.meta.IndexOffset
	}
	buf := make([]byte, endOffset-startOffset)
	_, err := s.file.ReadAt(buf, startOffset)
	if err != nil {
		return Entry{}, false, fmt.Errorf("error reading sstable entries from sstable file: %v", err)
	}
	pos := int64(0)
	for pos < int64(len(buf)) {
		keyLen := binary.BigEndian.Uint16(buf[pos : pos+2])
		pos += 2
		entryKey := make([]byte, keyLen)
		copy(entryKey, buf[pos:pos+int64(keyLen)])
		pos += int64(keyLen)
		valLen := binary.BigEndian.Uint32(buf[pos : pos+4])
		pos += 4
		if bytes.Equal(key, entryKey) {
			entryVal := make([]byte, valLen)
			copy(entryVal, buf[pos:pos+int64(valLen)])
			pos += int64(valLen)
			tombstone := bool(buf[pos] == 1)
			pos += 1
			timestamp := int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
			return Entry{
				key,
				entryVal,
				tombstone,
				timestamp,
			}, true, nil
		} else if bytes.Compare(key, entryKey) < 0 {
			break
		}
		pos += int64(valLen) + 1 + 8
	}
	return Entry{}, false, nil
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (s *SSTable) readEntryAt(offset int64) (Entry, int64, error) {
	hdr := make([]byte, 2)
	_, err := s.file.ReadAt(hdr, offset)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("read key length: %w", err)
	}
	keyLen := binary.BigEndian.Uint16(hdr)
	offset += 2

	buf := make([]byte, int(keyLen)+4)
	_, err = s.file.ReadAt(buf, offset)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("read key and value length: %w", err)
	}
	key := make([]byte, keyLen)
	copy(key, buf[:keyLen])
	valLen := binary.BigEndian.Uint32(buf[keyLen:])
	offset += int64(keyLen) + 4

	tail := make([]byte, int(valLen)+1+8)
	_, err = s.file.ReadAt(tail, offset)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("read value and metadata: %w", err)
	}
	value := make([]byte, valLen)
	copy(value, tail[:valLen])
	tombstone := tail[valLen] == 1
	timestamp := int64(binary.BigEndian.Uint64(tail[valLen+1:]))

	totalBytes := int64(2) + int64(keyLen) + 4 + int64(valLen) + 1 + 8
	return Entry{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Timestamp: timestamp,
	}, totalBytes, nil
}
