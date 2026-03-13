package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)

	wal, err := NewWAL(config)
	require.NoError(t, err)

	// Append 100 entries — mix of Put and Delete
	for i := 0; i < 100; i++ {
		entry := WALEntry{
			Key:   []byte(fmt.Sprintf("key-%03d", i)),
			Value: []byte(fmt.Sprintf("val-%03d", i)),
			Type:  EntryPut,
		}
		// Every 5th entry is a delete
		if i%5 == 0 {
			entry.Type = EntryDelete
			entry.Value = nil
		}
		idx, err := wal.Append(entry)
		require.NoError(t, err)
		assert.Equal(t, uint64(i+1), idx)
	}

	err = wal.Close()
	require.NoError(t, err)

	// Create new WAL with same directory — simulates restart
	wal2, err := NewWAL(config)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, 100, len(entries))

	for i, entry := range entries {
		expectedKey := []byte(fmt.Sprintf("key-%03d", i))
		assert.True(t, bytes.Equal(expectedKey, entry.Key),
			"entry %d: key mismatch: got %s, want %s", i, entry.Key, expectedKey)
		assert.Equal(t, uint64(i+1), entry.Index)

		if i%5 == 0 {
			assert.Equal(t, EntryDelete, entry.Type, "entry %d should be Delete", i)
			assert.Equal(t, 0, len(entry.Value), "entry %d delete should have empty value", i)
		} else {
			assert.Equal(t, EntryPut, entry.Type, "entry %d should be Put", i)
			expectedVal := []byte(fmt.Sprintf("val-%03d", i))
			assert.True(t, bytes.Equal(expectedVal, entry.Value),
				"entry %d: value mismatch", i)
		}
	}
}

func TestWALRecoverEmpty(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)

	wal, err := NewWAL(config)
	require.NoError(t, err)

	entries, err := wal.Recover()
	require.NoError(t, err)
	assert.Equal(t, 0, len(entries))

	wal.Close()
}

func TestWALCorruptionHandling(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)

	wal, err := NewWAL(config)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		_, err := wal.Append(WALEntry{
			Type:  EntryPut,
			Key:   []byte(fmt.Sprintf("key-%03d", i)),
			Value: []byte(fmt.Sprintf("val-%03d", i)),
		})
		require.NoError(t, err)
	}
	err = wal.Close()
	require.NoError(t, err)

	// Find the WAL segment file and corrupt the tail
	files, err := filepath.Glob(filepath.Join(config.WALDir, "wal-*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	// Open the last segment and append garbage
	f, err := os.OpenFile(files[len(files)-1], os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	// Write a fake TotalLen header pointing to 1000 bytes, followed by garbage
	// This simulates a crash mid-write: TotalLen was written but the payload wasn't
	_, err = f.Write([]byte{0x00, 0x00, 0x03, 0xE8, 0xFF, 0xFF, 0xFF, 0xFF, 0xAB})
	require.NoError(t, err)
	f.Close()

	// Recover should get exactly 50 entries — garbage is detected and skipped
	wal2, err := NewWAL(config)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.Recover()
	require.NoError(t, err)
	assert.Equal(t, 50, len(entries))

	// Verify the valid entries are intact
	for i, entry := range entries {
		expectedKey := []byte(fmt.Sprintf("key-%03d", i))
		assert.True(t, bytes.Equal(expectedKey, entry.Key))
	}
}

func TestWALConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)

	wal, err := NewWAL(config)
	require.NoError(t, err)

	var wg sync.WaitGroup
	goroutines := 10
	entriesPerGoroutine := 100

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				_, err := wal.Append(WALEntry{
					Type:  EntryPut,
					Key:   []byte(fmt.Sprintf("g%d-key-%03d", id, i)),
					Value: []byte(fmt.Sprintf("g%d-val-%03d", id, i)),
				})
				assert.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()

	err = wal.Close()
	require.NoError(t, err)

	// Recover and verify
	wal2, err := NewWAL(config)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.Recover()
	require.NoError(t, err)
	assert.Equal(t, goroutines*entriesPerGoroutine, len(entries))

	// All indices should be unique and in range [1, 1000]
	seen := make(map[uint64]bool)
	for _, entry := range entries {
		assert.False(t, seen[entry.Index], "duplicate index: %d", entry.Index)
		seen[entry.Index] = true
		assert.GreaterOrEqual(t, entry.Index, uint64(1))
		assert.LessOrEqual(t, entry.Index, uint64(goroutines*entriesPerGoroutine))
	}
	assert.Equal(t, goroutines*entriesPerGoroutine, len(seen))
}

func TestWALLargeValues(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)

	wal, err := NewWAL(config)
	require.NoError(t, err)

	// Create a 1MB value with a recognizable pattern
	bigVal := make([]byte, 1024*1024)
	for i := range bigVal {
		bigVal[i] = byte(i % 256)
	}

	_, err = wal.Append(WALEntry{
		Type:  EntryPut,
		Key:   []byte("big-key"),
		Value: bigVal,
	})
	require.NoError(t, err)

	err = wal.Close()
	require.NoError(t, err)

	// Recover and verify
	wal2, err := NewWAL(config)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))

	assert.True(t, bytes.Equal([]byte("big-key"), entries[0].Key))
	assert.True(t, bytes.Equal(bigVal, entries[0].Value),
		"1MB value mismatch after recovery")
}

func TestWALDeleteEntries(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)

	wal, err := NewWAL(config)
	require.NoError(t, err)

	// Put("a", "1"), Put("b", "2"), Delete("a"), Put("c", "3")
	wal.Append(WALEntry{Type: EntryPut, Key: []byte("a"), Value: []byte("1")})
	wal.Append(WALEntry{Type: EntryPut, Key: []byte("b"), Value: []byte("2")})
	wal.Append(WALEntry{Type: EntryDelete, Key: []byte("a"), Value: nil})
	wal.Append(WALEntry{Type: EntryPut, Key: []byte("c"), Value: []byte("3")})

	err = wal.Close()
	require.NoError(t, err)

	wal2, err := NewWAL(config)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, 4, len(entries))

	assert.Equal(t, EntryPut, entries[0].Type)
	assert.Equal(t, EntryPut, entries[1].Type)
	assert.Equal(t, EntryDelete, entries[2].Type)
	assert.True(t, bytes.Equal([]byte("a"), entries[2].Key))
	assert.Equal(t, 0, len(entries[2].Value))
	assert.Equal(t, EntryPut, entries[3].Type)
}
