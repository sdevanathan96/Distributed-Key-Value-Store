package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineGetAfterFlush(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 1024 // 1KB — forces frequent flushes

	engine, err := NewEngine(config)
	require.NoError(t, err)

	// Write 200 entries — will trigger many flushes
	numEntries := 200
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine.Put(key, val))
	}

	// Wait for background flushes to complete
	time.Sleep(1 * time.Second)

	// ALL entries must be readable
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expectedVal := []byte(fmt.Sprintf("value-%06d", i))
		val, err := engine.Get(key)
		require.NoError(t, err, "key-%06d should be readable after flush", i)
		assert.Equal(t, expectedVal, val)
	}

	engine.Close()
}

func TestEngineGetAfterFlushWithDeletes(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 1024

	engine, err := NewEngine(config)
	require.NoError(t, err)

	// Write 100 entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine.Put(key, val))
	}

	engine.WaitForBackground()

	// Delete even-numbered keys
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		require.NoError(t, engine.Delete(key))
	}

	engine.WaitForBackground()

	// Even keys should be not found (tombstone stops search)
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		_, err := engine.Get(key)
		assert.ErrorIs(t, err, ErrKeyNotFound,
			"key-%06d should be deleted", i)
	}

	// Odd keys should still be readable
	for i := 1; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val, err := engine.Get(key)
		require.NoError(t, err, "key-%06d should still exist", i)
		assert.Equal(t, []byte(fmt.Sprintf("value-%06d", i)), val)
	}

	engine.Close()
}

func TestEngineRecoveryWithSSTables(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 1024

	// Phase 1: Write data, let some flush to SSTables
	engine1, err := NewEngine(config)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine1.Put(key, val))
	}

	time.Sleep(500 * time.Millisecond)

	// Write more data (this will be in WAL only, not yet flushed)
	for i := 100; i < 120; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine1.Put(key, val))
	}

	engine1.Close()

	// Phase 2: Restart — should recover from SSTables + WAL
	engine2, err := NewEngine(config)
	require.NoError(t, err)
	defer engine2.Close()

	// All 120 entries should be readable
	for i := 0; i < 120; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expectedVal := []byte(fmt.Sprintf("value-%06d", i))
		val, err := engine2.Get(key)
		require.NoError(t, err, "key-%06d should be recovered", i)
		assert.Equal(t, expectedVal, val)
	}
}

func TestEngineOverwriteAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 512 // very small

	engine, err := NewEngine(config)
	require.NoError(t, err)

	// Write key "counter" 50 times with increasing values
	// This will span multiple flushes
	for i := 0; i < 50; i++ {
		val := []byte(fmt.Sprintf("version-%d", i))
		require.NoError(t, engine.Put([]byte("counter"), val))
	}

	time.Sleep(1 * time.Second)

	// Should return the latest version
	val, err := engine.Get([]byte("counter"))
	require.NoError(t, err)
	assert.Equal(t, "version-49", string(val))

	engine.Close()
}
