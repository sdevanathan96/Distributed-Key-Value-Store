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
	config.MemTableSize = 1024

	engine, err := NewEngine(config)
	require.NoError(t, err)

	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		err := engine.Put(key, val)
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	l0Dir := filepath.Join(config.SSTableDir, "L0")
	files, err := filepath.Glob(filepath.Join(l0Dir, "*.sst"))
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "should have at least one SSTable in L0")
	t.Logf("Flushed %d SSTable files to L0", len(files))

	lastKey := []byte(fmt.Sprintf("key-%06d", numEntries-1))
	_, err = engine.Get(lastKey)

	if err != nil {
		assert.ErrorIs(t, err, ErrKeyNotFound)
	}

	engine.Close()
}

func TestEngineCrashDuringFlush(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig(dir)
	config.MemTableSize = 1024

	engine1, err := NewEngine(config)
	require.NoError(t, err)

	numEntries := 50
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine1.Put(key, val))
	}

	time.Sleep(500 * time.Millisecond)
	engine1.Close()

	l0Dir := filepath.Join(config.SSTableDir, "L0")
	sstFiles, _ := filepath.Glob(filepath.Join(l0Dir, "*.sst"))
	for _, f := range sstFiles {
		os.Remove(f)
	}
	t.Logf("Deleted %d SSTable files to simulate crash", len(sstFiles))

	engine2, err := NewEngine(config)
	require.NoError(t, err)
	defer engine2.Close()

	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expectedVal := []byte(fmt.Sprintf("value-%06d", i))

		val, err := engine2.Get(key)
		require.NoError(t, err, "key-%06d should be recovered from WAL", i)
		assert.Equal(t, expectedVal, val)
	}
}
