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
	config.MemTableSize = 1024

	engine, err := NewEngine(config)
	require.NoError(t, err)

	numEntries := 200
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine.Put(key, val))
	}

	time.Sleep(1 * time.Second)

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

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine.Put(key, val))
	}

	engine.WaitForBackground()

	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		require.NoError(t, engine.Delete(key))
	}

	engine.WaitForBackground()

	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		_, err := engine.Get(key)
		assert.ErrorIs(t, err, ErrKeyNotFound,
			"key-%06d should be deleted", i)
	}

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

	engine1, err := NewEngine(config)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine1.Put(key, val))
	}

	time.Sleep(500 * time.Millisecond)

	for i := 100; i < 120; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, engine1.Put(key, val))
	}

	engine1.Close()

	engine2, err := NewEngine(config)
	require.NoError(t, err)
	defer engine2.Close()

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
	config.MemTableSize = 512

	engine, err := NewEngine(config)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		val := []byte(fmt.Sprintf("version-%d", i))
		require.NoError(t, engine.Put([]byte("counter"), val))
	}

	time.Sleep(1 * time.Second)

	val, err := engine.Get([]byte("counter"))
	require.NoError(t, err)
	assert.Equal(t, "version-49", string(val))

	engine.Close()
}
