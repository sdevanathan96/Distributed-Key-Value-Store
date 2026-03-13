package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemTablePutGet(t *testing.T) {
	mem := NewMemTable(1024 * 1024) // 1MB — plenty of room

	// Put 10 entries
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		mem.Put(key, val)
	}

	// Get each one back
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		expectedVal := []byte(fmt.Sprintf("val-%03d", i))
		val, found := mem.Get(key)
		require.True(t, found, "key-%03d should exist", i)
		assert.True(t, bytes.Equal(expectedVal, val))
	}

	// Non-existent key
	val, found := mem.Get([]byte("nonexistent"))
	assert.False(t, found)
	assert.Nil(t, val)

	assert.Equal(t, 10, mem.Len())
}

func TestMemTableOverwrite(t *testing.T) {
	mem := NewMemTable(1024 * 1024)

	mem.Put([]byte("name"), []byte("alice"))
	sizeBefore := mem.size

	mem.Put([]byte("name"), []byte("bob"))
	sizeAfter := mem.size

	// Should return latest value
	val, found := mem.Get([]byte("name"))
	require.True(t, found)
	assert.True(t, bytes.Equal([]byte("bob"), val))

	// Count should be 1, not 2
	assert.Equal(t, 1, mem.Len())

	// Size should reflect one entry, not two
	// "bob" is shorter than "alice", but with overhead the difference is small
	// Just verify size didn't double
	oneEntryEstimate := estimateSize([]byte("name"), []byte("bob"))
	assert.Equal(t, oneEntryEstimate, sizeAfter)
	_ = sizeBefore // used implicitly — size should have adjusted
}

func TestMemTableDelete(t *testing.T) {
	mem := NewMemTable(1024 * 1024)

	mem.Put([]byte("key"), []byte("value"))
	val, found := mem.Get([]byte("key"))
	require.True(t, found)
	assert.True(t, bytes.Equal([]byte("value"), val))

	// Delete the key
	mem.Delete([]byte("key"))

	// Get should return not found (tombstone hides the value)
	val, found = mem.Get([]byte("key"))
	assert.False(t, found)
	assert.Nil(t, val)

	// But the entry still EXISTS in the tree as a tombstone
	// Entries() should return it
	entries := mem.Entries()
	require.Equal(t, 1, len(entries))
	assert.True(t, entries[0].Tombstone)
	assert.True(t, bytes.Equal([]byte("key"), entries[0].Key))
}

func TestMemTableOrdering(t *testing.T) {
	mem := NewMemTable(1024 * 1024)

	// Insert in deliberately unsorted order
	keys := []string{"banana", "date", "apple", "cherry", "elderberry"}
	for _, k := range keys {
		mem.Put([]byte(k), []byte("val-"+k))
	}

	entries := mem.Entries()
	require.Equal(t, len(keys), len(entries))

	// Verify sorted order
	expected := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, entry := range entries {
		assert.True(t, bytes.Equal([]byte(expected[i]), entry.Key),
			"position %d: got %s, want %s", i, entry.Key, expected[i])
	}
}

func TestMemTableShouldFlush(t *testing.T) {
	mem := NewMemTable(1024) // Small threshold: 1KB

	// Each entry is roughly: len(key) + len(value) + 41 overhead
	// With key ~10 bytes and value ~10 bytes: ~61 bytes per entry
	// Should flush around 1024/61 ≈ 16-17 entries

	flushedAt := -1
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))

		wasFlushing := mem.ShouldFlush()
		mem.Put(key, val)

		if mem.ShouldFlush() && flushedAt == -1 {
			flushedAt = i
		}

		// Before the first flush trigger, ShouldFlush should have been false
		if flushedAt == -1 {
			assert.False(t, wasFlushing, "shouldn't need flush at entry %d", i)
		}
	}

	require.NotEqual(t, -1, flushedAt, "ShouldFlush should have triggered")
	// Should trigger somewhere in a reasonable range
	assert.Greater(t, flushedAt, 5, "triggered too early")
	assert.Less(t, flushedAt, 30, "triggered too late")
}

func TestMemTableTombstoneOverwrite(t *testing.T) {
	mem := NewMemTable(1024 * 1024)

	// Put → Delete → Put should restore the key
	mem.Put([]byte("a"), []byte("1"))
	val, found := mem.Get([]byte("a"))
	require.True(t, found)
	assert.True(t, bytes.Equal([]byte("1"), val))

	mem.Delete([]byte("a"))
	val, found = mem.Get([]byte("a"))
	assert.False(t, found)

	mem.Put([]byte("a"), []byte("2"))
	val, found = mem.Get([]byte("a"))
	require.True(t, found)
	assert.True(t, bytes.Equal([]byte("2"), val))

	// Should still be 1 entry, not 3
	assert.Equal(t, 1, mem.Len())

	// The entry should NOT be a tombstone
	entries := mem.Entries()
	require.Equal(t, 1, len(entries))
	assert.False(t, entries[0].Tombstone)
}
