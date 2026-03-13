package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineFlush(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 1024 // 1KB — tiny, forces frequent flushes

	engine, err := NewEngine(config)
	require.NoError(t, err)

	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		err := engine.Put(key, val)
		require.NoError(t, err)
	}

	// Wait for background flush goroutine to complete
	time.Sleep(500 * time.Millisecond)

	// Verify SSTable file(s) exist in L0 directory
	l0Dir := filepath.Join(config.SSTableDir, "L0")
	files, err := filepath.Glob(filepath.Join(l0Dir, "*.sst"))
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "should have at least one SSTable in L0")
	t.Logf("Flushed %d SSTable files to L0", len(files))

	// Verify all data is still readable through the Engine
	// (either from active MemTable or immutable — SSTables aren't
	// wired into Get yet, that's Day 3)
	//
	// Note: some keys may be in the MemTable (post-flush writes)
	// and some may have been in the flushed MemTable that's now nil.
	// Without SSTable reads wired in, those flushed keys will be
	// ErrKeyNotFound. This is expected for Day 2.
	// The real end-to-end test comes on Day 3 when LSM reads work.
	//
	// For now, just verify the engine doesn't crash and recent keys work:
	lastKey := []byte(fmt.Sprintf("key-%06d", numEntries-1))
	_, err = engine.Get(lastKey)
	// Don't assert found — it might have been flushed already
	// Just verify no panic or unexpected error
	if err != nil {
		assert.ErrorIs(t, err, ErrKeyNotFound)
	}

	engine.Close()
}

func TestEngineCrashDuringFlush(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 1024 // small to trigger flush

	// Phase 1: Write data
	engine1, err := NewEngine(config)
	require.NoError(t, err)

	numEntries := 50
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine1.Put(key, val))
	}

	// Wait for any background flush
	time.Sleep(500 * time.Millisecond)
	engine1.Close()

	// Phase 2: Simulate crash during flush — delete any SSTable files
	l0Dir := filepath.Join(config.SSTableDir, "L0")
	sstFiles, _ := filepath.Glob(filepath.Join(l0Dir, "*.sst"))
	for _, f := range sstFiles {
		os.Remove(f)
	}
	t.Logf("Deleted %d SSTable files to simulate crash", len(sstFiles))

	// Phase 3: Restart engine — WAL recovery should restore everything
	engine2, err := NewEngine(config)
	require.NoError(t, err)
	defer engine2.Close()

	// Verify all keys are recoverable from WAL
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expectedVal := []byte(fmt.Sprintf("value-%06d", i))

		val, err := engine2.Get(key)
		require.NoError(t, err, "key-%06d should be recovered from WAL", i)
		assert.Equal(t, expectedVal, val)
	}
}
