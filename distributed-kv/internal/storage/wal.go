package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type WAL struct {
	mu              sync.Mutex
	file            *os.File
	dir             string
	nextIndex       uint64
	size            int64
	segmentNum      uint64
	truncatedBefore uint64
	config          StorageConfig
	closed          bool
}

func NewWAL(config StorageConfig) (*WAL, error) {
	err := os.MkdirAll(config.WALDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	files, err := filepath.Glob(filepath.Join(config.WALDir, "wal-*.log"))
	if err != nil {
		return nil, fmt.Errorf("search wal dir: %w", err)
	}
	var segNum uint64
	segNum = 0
	if len(files) > 0 {
		for _, path := range files {
			var segmentNumber uint64
			file := filepath.Base(path)
			_, err := fmt.Sscanf(file, "wal-%d.log", &segmentNumber)
			if err != nil {
				return nil, fmt.Errorf("parse wal segment number: %w", err)
			}
			if segNum < segmentNumber {
				segNum = segmentNumber
			}
		}
	}
	segNum++
	filename := fmt.Sprintf("wal-%06d.log", segNum)
	path := filepath.Join(config.WALDir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("wal stat: %w", err)
	}
	size := info.Size()
	var trunc uint64 = 0

	type walMeta struct {
		TruncatedBefore uint64 `json:"truncatedBefore"`
	}

	metaPath := filepath.Join(config.WALDir, "wal-meta.json")
	if _, statErr := os.Stat(metaPath); statErr == nil {
		data, readErr := os.ReadFile(metaPath)
		if readErr != nil {
			return nil, fmt.Errorf("read wal-meta: %w", readErr)
		}
		var meta walMeta
		if jsonErr := json.Unmarshal(data, &meta); jsonErr != nil {
			return nil, fmt.Errorf("parse wal-meta: %w", jsonErr)
		}
		trunc = meta.TruncatedBefore
	}

	return &WAL{
		file:            file,
		dir:             config.WALDir,
		nextIndex:       1,
		size:            size,
		config:          config,
		segmentNum:      segNum,
		truncatedBefore: trunc,
		closed:          false,
	}, err
}

func (w *WAL) Append(entry WALEntry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWALClosed
	}
	entry.Index = w.nextIndex
	w.nextIndex++
	payloadSize := 8 + 8 + 1 + 2 + len(entry.Key) + len(entry.Value)
	payload := make([]byte, payloadSize)
	offset := 0
	binary.BigEndian.PutUint64(payload[offset:], entry.Term)
	offset += 8
	binary.BigEndian.PutUint64(payload[offset:], entry.Index)
	offset += 8
	payload[offset] = byte(entry.Type)
	offset += 1
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(entry.Key)))
	offset += 2
	copy(payload[offset:], entry.Key)
	offset += len(entry.Key)
	copy(payload[offset:], entry.Value)
	checksum := crc32.ChecksumIEEE(payload)
	totalLen := uint32(4 + len(payload))
	record := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(record[0:4], totalLen)
	binary.BigEndian.PutUint32(record[4:8], checksum)
	copy(record[8:], payload)
	n, err := w.file.Write(record)
	if err != nil {
		return 0, fmt.Errorf("wal write: %w", err)
	}
	if w.config.SyncWrites {
		if err := w.file.Sync(); err != nil {
			return 0, fmt.Errorf("wal sync: %w", err)
		}
	}
	w.size += int64(n)
	if w.size >= w.config.WALMaxSize {
		if err := w.rotate(); err != nil {
			return 0, fmt.Errorf("wal rotate: %w", err)
		}
	}
	return entry.Index, nil
}

func (w *WAL) Recover() ([]WALEntry, error) {
	var maxEntryIndex uint64 = 0
	entries := make([]WALEntry, 0)

	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return nil, fmt.Errorf("find wal dir: %w", err)
	}
	sort.Strings(files)
	for _, path := range files {
		reader, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open wal file: %w", err)
		}
		for {
			lenBuf := make([]byte, 4)
			_, err = io.ReadFull(reader, lenBuf)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			totalLen := binary.BigEndian.Uint32(lenBuf)

			data := make([]byte, totalLen)
			_, err = io.ReadFull(reader, data)
			if err != nil {
				break
			}
			storedCRC := binary.BigEndian.Uint32(data[0:4])
			payload := data[4:]
			computedCRC := crc32.ChecksumIEEE(payload)
			if storedCRC != computedCRC {
				break
			}
			entry := decodePayload(payload)
			if entry.Index <= w.truncatedBefore {
				continue
			}
			entries = append(entries, entry)

			if maxEntryIndex < entry.Index {
				maxEntryIndex = entry.Index
			}
		}
		err = reader.Close()
		if err != nil {
			return nil, fmt.Errorf("close wal file: %w", err)
		}
	}
	w.nextIndex = maxEntryIndex + 1
	return entries, err
}

func decodePayload(payload []byte) WALEntry {
	entry := WALEntry{}
	term := binary.BigEndian.Uint64(payload[0:8])
	index := binary.BigEndian.Uint64(payload[8:16])
	entry.Term = term
	entry.Index = index
	entry.Type = EntryType(payload[16])
	keyLen := binary.BigEndian.Uint16(payload[17:19])
	entry.Key = make([]byte, keyLen)
	copy(entry.Key, payload[19:19+keyLen])
	valueLen := len(payload) - 19 - int(keyLen)
	entry.Value = make([]byte, valueLen)
	copy(entry.Value, payload[19+keyLen:])
	return entry
}

func (w *WAL) Truncate(afterIndex uint64) error {
	filename := fmt.Sprintf("wal-meta.json")
	path := filepath.Join(w.config.WALDir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open wal-meta file: %w", err)
	}
	f := map[string]interface{}{
		"truncatedBefore": afterIndex,
	}
	jsonData, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	write, err := file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("wal-meta write: %w", err)
	}
	if write != len(jsonData) {
		return fmt.Errorf("write wrong")
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("close wal-meta file: %w", err)
	}
	w.truncatedBefore = afterIndex
	return err
}

func (w *WAL) rotate() error {
	err := w.file.Sync()
	if err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	err = w.file.Close()
	if err != nil {
		return fmt.Errorf("wal close: %w", err)
	}
	nextSegNum := w.segmentNum + 1
	filename := fmt.Sprintf("wal-%06d.log", nextSegNum)
	path := filepath.Join(w.config.WALDir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open wal file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("wal stat: %w", err)
	}
	size := info.Size()
	w.size = size
	w.segmentNum = nextSegNum
	w.file = file
	return err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	err := w.file.Sync()
	if err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	err = w.file.Close()
	if err != nil {
		return fmt.Errorf("wal close: %w", err)
	}
	return err
}
