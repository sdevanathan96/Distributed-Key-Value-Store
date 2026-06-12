package lsm

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	numEntries := 1000
	writer, err := NewSSTableWriter(path, uint(numEntries))
	require.NoError(t, err)

	for i := 0; i < numEntries; i++ {
		entry := Entry{
			Key:       []byte(fmt.Sprintf("key-%06d", i)),
			Value:     []byte(fmt.Sprintf("value-%06d", i)),
			Tombstone: false,
			Timestamp: int64(i),
		}
		err := writer.WriteEntry(entry)
		require.NoError(t, err)
	}

	meta, err := writer.Finish()
	require.NoError(t, err)
	assert.Equal(t, int64(numEntries), meta.EntryCount)
	assert.Equal(t, []byte("key-000000"), meta.MinKey)
	assert.Equal(t, []byte(fmt.Sprintf("key-%06d", numEntries-1)), meta.MaxKey)

	sst, err := OpenSSTable(path, 0)
	require.NoError(t, err)
	defer sst.Close()

	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expectedVal := []byte(fmt.Sprintf("value-%06d", i))

		entry, found, err := sst.Get(key)
		require.NoError(t, err)
		assert.True(t, found, "key %s should be found", key)
		assert.Equal(t, expectedVal, entry.Value)
		assert.False(t, entry.Tombstone)
		assert.Equal(t, int64(i), entry.Timestamp)
	}

	missingKeys := []string{"aaa-missing", "key-999999", "zzz-missing"}
	for _, k := range missingKeys {
		_, found, err := sst.Get([]byte(k))
		require.NoError(t, err)
		assert.False(t, found, "key %s should not be found", k)
	}
}

func TestSSTableBloomFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bloom_test.sst")

	numEntries := 10000
	writer, err := NewSSTableWriter(path, uint(numEntries))
	require.NoError(t, err)

	for i := 0; i < numEntries; i++ {
		entry := Entry{
			Key:       []byte(fmt.Sprintf("exists-%06d", i)),
			Value:     []byte(fmt.Sprintf("val-%d", i)),
			Tombstone: false,
			Timestamp: int64(i),
		}
		require.NoError(t, writer.WriteEntry(entry))
	}
	_, err = writer.Finish()
	require.NoError(t, err)

	sst, err := OpenSSTable(path, 0)
	require.NoError(t, err)
	defer sst.Close()

	falsePositives := 0
	numTests := 10000
	for i := 0; i < numTests; i++ {
		key := []byte(fmt.Sprintf("missing-%06d", i))

		if sst.bloom.MayContain(key) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(numTests)
	t.Logf("Bloom filter false positive rate: %.2f%% (%d / %d)", fpRate*100, falsePositives, numTests)
	assert.Less(t, fpRate, 0.02, "false positive rate should be under 2%%")
}

func TestSSTableTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tombstone_test.sst")

	writer, err := NewSSTableWriter(path, 100)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		entry := Entry{
			Key:       []byte(fmt.Sprintf("key-%06d", i)),
			Value:     nil,
			Tombstone: i%2 == 1,
			Timestamp: int64(i),
		}
		if !entry.Tombstone {
			entry.Value = []byte(fmt.Sprintf("value-%d", i))
		}
		require.NoError(t, writer.WriteEntry(entry))
	}
	_, err = writer.Finish()
	require.NoError(t, err)

	sst, err := OpenSSTable(path, 0)
	require.NoError(t, err)
	defer sst.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		entry, found, err := sst.Get(key)
		require.NoError(t, err)
		require.True(t, found, "key %s should be found", key)

		if i%2 == 1 {
			assert.True(t, entry.Tombstone, "key-%06d should be a tombstone", i)
			assert.Equal(t, 0, len(entry.Value), "tombstone should have empty value")
		} else {
			assert.False(t, entry.Tombstone, "key-%06d should not be a tombstone", i)
			assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), entry.Value)
		}
	}
}
