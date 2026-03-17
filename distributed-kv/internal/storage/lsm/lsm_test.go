package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

func writeSSTable(t *testing.T, dir string, id int, entries []Entry) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0755))
	path := filepath.Join(dir, fmt.Sprintf("%06d.sst", id))
	writer, err := NewSSTableWriter(path, uint(len(entries)))
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, writer.WriteEntry(e))
	}
	_, err = writer.Finish()
	require.NoError(t, err)
	return path
}

func makeEntry(key, value string, ts int64) Entry {
	return Entry{
		Key:       []byte(key),
		Value:     []byte(value),
		Tombstone: false,
		Timestamp: ts,
	}
}

func makeTombstone(key string, ts int64) Entry {
	return Entry{
		Key:       []byte(key),
		Value:     nil,
		Tombstone: true,
		Timestamp: ts,
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// LEVEL TESTS
// ═══════════════════════════════════════════════════════════════════════════

func TestLevelL0NewestFirst(t *testing.T) {
	dir := t.TempDir()
	l0 := NewLevel(0)

	path1 := writeSSTable(t, dir, 1, []Entry{
		makeEntry("a", "old_value", 100),
	})
	sst1, err := OpenSSTable(path1, 0)
	require.NoError(t, err)
	l0.AddSSTable(sst1)

	path2 := writeSSTable(t, dir, 2, []Entry{
		makeEntry("a", "new_value", 200),
	})
	sst2, err := OpenSSTable(path2, 0)
	require.NoError(t, err)
	l0.AddSSTable(sst2)

	entry, found, err := l0.Get([]byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("new_value"), entry.Value)
	assert.Equal(t, int64(200), entry.Timestamp)

	sst1.Close()
	sst2.Close()
}

func TestLevelL1SortedNonOverlapping(t *testing.T) {
	dir := t.TempDir()
	l1 := NewLevel(1)

	path1 := writeSSTable(t, dir, 1, []Entry{
		makeEntry("a", "v1", 100),
		makeEntry("b", "v2", 100),
		makeEntry("c", "v3", 100),
	})
	sst1, err := OpenSSTable(path1, 1)
	require.NoError(t, err)
	l1.AddSSTable(sst1)

	path2 := writeSSTable(t, dir, 2, []Entry{
		makeEntry("d", "v4", 100),
		makeEntry("e", "v5", 100),
		makeEntry("f", "v6", 100),
	})
	sst2, err := OpenSSTable(path2, 1)
	require.NoError(t, err)
	l1.AddSSTable(sst2)

	entry, found, err := l1.Get([]byte("b"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("v2"), entry.Value)

	entry, found, err = l1.Get([]byte("e"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("v5"), entry.Value)

	_, found, err = l1.Get([]byte("z"))
	require.NoError(t, err)
	assert.False(t, found)

	sst1.Close()
	sst2.Close()
}

// ═══════════════════════════════════════════════════════════════════════════
// LSM TREE TESTS
// ═══════════════════════════════════════════════════════════════════════════

func TestLSMTreeGet(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	lsmTree, err := NewLSMTree(sstDir, dir)
	require.NoError(t, err)
	defer lsmTree.Close()

	l0Dir := filepath.Join(sstDir, "L0")

	path1 := writeSSTable(t, l0Dir, 1, []Entry{
		makeEntry("a", "1", 100),
		makeEntry("b", "2", 100),
		makeEntry("c", "3", 100),
	})
	sst1, err := OpenSSTable(path1, 0)
	require.NoError(t, err)
	require.NoError(t, lsmTree.AddSSTable(sst1, 0))

	path2 := writeSSTable(t, l0Dir, 2, []Entry{
		makeEntry("b", "updated", 200),
		makeEntry("d", "4", 200),
	})
	sst2, err := OpenSSTable(path2, 0)
	require.NoError(t, err)
	require.NoError(t, lsmTree.AddSSTable(sst2, 0))

	entry, found, err := lsmTree.Get([]byte("b"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("updated"), entry.Value)

	entry, found, err = lsmTree.Get([]byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("1"), entry.Value)

	entry, found, err = lsmTree.Get([]byte("d"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("4"), entry.Value)

	_, found, err = lsmTree.Get([]byte("z"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestLSMTombstoneStopsSearch(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	lsmTree, err := NewLSMTree(sstDir, dir)
	require.NoError(t, err)
	defer lsmTree.Close()

	l0Dir := filepath.Join(sstDir, "L0")

	path1 := writeSSTable(t, l0Dir, 1, []Entry{
		makeEntry("name", "alice", 100),
	})
	sst1, err := OpenSSTable(path1, 0)
	require.NoError(t, err)
	require.NoError(t, lsmTree.AddSSTable(sst1, 0))

	path2 := writeSSTable(t, l0Dir, 2, []Entry{
		makeTombstone("name", 200),
	})
	sst2, err := OpenSSTable(path2, 0)
	require.NoError(t, err)
	require.NoError(t, lsmTree.AddSSTable(sst2, 0))

	entry, found, err := lsmTree.Get([]byte("name"))
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, entry.Tombstone, "should return tombstone entry")
}

// ═══════════════════════════════════════════════════════════════════════════
// MANIFEST TESTS
// ═══════════════════════════════════════════════════════════════════════════

func TestManifestPersistence(t *testing.T) {
	dir := t.TempDir()

	m1 := NewManifest(dir)
	require.NoError(t, m1.AddSSTable("/data/L0/000001.sst", 0))
	require.NoError(t, m1.AddSSTable("/data/L0/000002.sst", 0))
	id1 := m1.GetNextSSTableID()
	id2 := m1.GetNextSSTableID()

	assert.Equal(t, uint64(1), id1)
	assert.Equal(t, uint64(2), id2)

	m2 := NewManifest(dir)
	sstables := m2.GetSSTables()
	assert.Equal(t, 2, len(sstables))
	assert.Equal(t, "/data/L0/000001.sst", sstables[0].Path)
	assert.Equal(t, "/data/L0/000002.sst", sstables[1].Path)

	id3 := m2.GetNextSSTableID()
	assert.Equal(t, uint64(3), id3)
}

func TestManifestRemove(t *testing.T) {
	dir := t.TempDir()
	m := NewManifest(dir)

	require.NoError(t, m.AddSSTable("/data/L0/000001.sst", 0))
	require.NoError(t, m.AddSSTable("/data/L0/000002.sst", 0))
	require.NoError(t, m.AddSSTable("/data/L1/000003.sst", 1))

	require.NoError(t, m.RemoveSSTable("/data/L0/000001.sst"))

	sstables := m.GetSSTables()
	assert.Equal(t, 2, len(sstables))
	assert.Equal(t, "/data/L0/000002.sst", sstables[0].Path)
	assert.Equal(t, "/data/L1/000003.sst", sstables[1].Path)

	m2 := NewManifest(dir)
	assert.Equal(t, 2, len(m2.GetSSTables()))
}

// ═══════════════════════════════════════════════════════════════════════════
// ITERATOR TESTS
// ═══════════════════════════════════════════════════════════════════════════

func TestSSTableIterator(t *testing.T) {
	dir := t.TempDir()
	entries := []Entry{
		makeEntry("a", "1", 100),
		makeEntry("b", "2", 100),
		makeEntry("c", "3", 100),
		makeEntry("d", "4", 100),
		makeEntry("e", "5", 100),
	}
	path := writeSSTable(t, dir, 1, entries)
	sst, err := OpenSSTable(path, 0)
	require.NoError(t, err)
	defer sst.Close()

	iter, err := NewSSTableIterator(sst)
	require.NoError(t, err)

	var results []Entry
	for iter.Valid() {
		results = append(results, iter.Entry())
		iter.Next()
	}

	require.Equal(t, len(entries), len(results))
	for i, e := range results {
		assert.Equal(t, entries[i].Key, e.Key)
		assert.Equal(t, entries[i].Value, e.Value)
	}
}

func TestMergeIteratorDeduplication(t *testing.T) {
	dir := t.TempDir()

	// SST-1 (older): a=old, c=v3, e=v5
	path1 := writeSSTable(t, dir, 1, []Entry{
		makeEntry("a", "old", 100),
		makeEntry("c", "v3", 100),
		makeEntry("e", "v5", 100),
	})
	sst1, err := OpenSSTable(path1, 0)
	require.NoError(t, err)
	defer sst1.Close()

	// SST-2 (newer): a=new, b=v2, c=v3_updated
	path2 := writeSSTable(t, dir, 2, []Entry{
		makeEntry("a", "new", 200),
		makeEntry("b", "v2", 200),
		makeEntry("c", "v3_updated", 200),
	})
	sst2, err := OpenSSTable(path2, 0)
	require.NoError(t, err)
	defer sst2.Close()

	iter1, err := NewSSTableIterator(sst1)
	require.NoError(t, err)
	iter2, err := NewSSTableIterator(sst2)
	require.NoError(t, err)

	merger := NewMergeIterator([]*SSTableIterator{iter1, iter2})

	var results []Entry
	for {
		entry, ok := merger.Next()
		if !ok {
			break
		}
		results = append(results, entry)
	}

	// Unique keys: a, b, c, e = 4 (a and c are deduplicated)
	require.Equal(t, 4, len(results))

	assert.Equal(t, "a", string(results[0].Key))
	assert.Equal(t, "new", string(results[0].Value))
	assert.Equal(t, int64(200), results[0].Timestamp)

	assert.Equal(t, "b", string(results[1].Key))
	assert.Equal(t, "v2", string(results[1].Value))

	assert.Equal(t, "c", string(results[2].Key))
	assert.Equal(t, "v3_updated", string(results[2].Value))

	assert.Equal(t, "e", string(results[3].Key))
	assert.Equal(t, "v5", string(results[3].Value))
}

func TestMergeIteratorWithTombstones(t *testing.T) {
	dir := t.TempDir()

	path1 := writeSSTable(t, dir, 1, []Entry{
		makeEntry("a", "alive", 100),
		makeEntry("b", "alive", 100),
	})
	sst1, err := OpenSSTable(path1, 0)
	require.NoError(t, err)
	defer sst1.Close()

	path2 := writeSSTable(t, dir, 2, []Entry{
		makeTombstone("a", 200),
	})
	sst2, err := OpenSSTable(path2, 0)
	require.NoError(t, err)
	defer sst2.Close()

	iter1, err := NewSSTableIterator(sst1)
	require.NoError(t, err)
	iter2, err := NewSSTableIterator(sst2)
	require.NoError(t, err)

	merger := NewMergeIterator([]*SSTableIterator{iter1, iter2})

	var results []Entry
	for {
		entry, ok := merger.Next()
		if !ok {
			break
		}
		results = append(results, entry)
	}

	require.Equal(t, 2, len(results))
	assert.Equal(t, "a", string(results[0].Key))
	assert.True(t, results[0].Tombstone, "a should be tombstone")
	assert.Equal(t, "b", string(results[1].Key))
	assert.False(t, results[1].Tombstone)
}

// ═══════════════════════════════════════════════════════════════════════════
// COMPACTION TESTS
// ═══════════════════════════════════════════════════════════════════════════

func TestCompactionBasic(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	l0Dir := filepath.Join(sstDir, "L0")

	lsmTree, err := NewLSMTree(sstDir, dir)
	require.NoError(t, err)
	defer lsmTree.Close()

	for s := 0; s < 5; s++ {
		var entries []Entry
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%04d", s*20+i)
			val := fmt.Sprintf("val-%d-sst%d", i, s)
			entries = append(entries, makeEntry(key, val, int64(s*1000+i)))
		}
		path := writeSSTable(t, l0Dir, s+1, entries)
		sst, err := OpenSSTable(path, 0)
		require.NoError(t, err)
		require.NoError(t, lsmTree.AddSSTable(sst, 0))
	}

	lsmTree.WaitForBackground()

	l0Count := lsmTree.GetLevel(0).SSTableCount()
	t.Logf("L0 SSTable count after compaction: %d", l0Count)
	assert.Less(t, l0Count, 5, "L0 should have fewer SSTables after compaction")

	l1Count := lsmTree.GetLevel(1).SSTableCount()
	t.Logf("L1 SSTable count after compaction: %d", l1Count)
	assert.Greater(t, l1Count, 0, "L1 should have SSTables after compaction")

	for s := 0; s < 5; s++ {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%04d", s*20+i)
			entry, found, err := lsmTree.Get([]byte(key))
			require.NoError(t, err)
			assert.True(t, found, "key %s should be found after compaction", key)
			assert.False(t, entry.Tombstone)
		}
	}
}

func TestCompactionOverwrittenKeys(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	l0Dir := filepath.Join(sstDir, "L0")

	lsmTree, err := NewLSMTree(sstDir, dir)
	require.NoError(t, err)
	defer lsmTree.Close()

	for s := 0; s < 5; s++ {
		entries := []Entry{
			makeEntry("shared", fmt.Sprintf("version-%d", s), int64(s*100)),
		}
		path := writeSSTable(t, l0Dir, s+1, entries)
		sst, err := OpenSSTable(path, 0)
		require.NoError(t, err)
		require.NoError(t, lsmTree.AddSSTable(sst, 0))
	}

	lsmTree.WaitForBackground()

	entry, found, err := lsmTree.Get([]byte("shared"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "version-4", string(entry.Value), "should have newest value after compaction")
}

func TestCompactionWithTombstones(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	l0Dir := filepath.Join(sstDir, "L0")

	lsmTree, err := NewLSMTree(sstDir, dir)
	require.NoError(t, err)
	defer lsmTree.Close()

	for s := 0; s < 3; s++ {
		entries := []Entry{
			makeEntry(fmt.Sprintf("key-%d", s), fmt.Sprintf("val-%d", s), int64(s*100)),
		}
		path := writeSSTable(t, l0Dir, s+1, entries)
		sst, err := OpenSSTable(path, 0)
		require.NoError(t, err)
		require.NoError(t, lsmTree.AddSSTable(sst, 0))
	}

	entries4 := []Entry{makeTombstone("key-0", 500)}
	path4 := writeSSTable(t, l0Dir, 4, entries4)
	sst4, err := OpenSSTable(path4, 0)
	require.NoError(t, err)
	require.NoError(t, lsmTree.AddSSTable(sst4, 0))

	entries5 := []Entry{makeTombstone("key-1", 600)}
	path5 := writeSSTable(t, l0Dir, 5, entries5)
	sst5, err := OpenSSTable(path5, 0)
	require.NoError(t, err)
	require.NoError(t, lsmTree.AddSSTable(sst5, 0))

	lsmTree.WaitForBackground()

	entry, found, err := lsmTree.Get([]byte("key-0"))
	require.NoError(t, err)
	if found {
		assert.True(t, entry.Tombstone, "key-0 should be tombstone after compaction")
	}

	entry, found, err = lsmTree.Get([]byte("key-2"))
	require.NoError(t, err)
	require.True(t, found)
	assert.False(t, entry.Tombstone)
	assert.Equal(t, "val-2", string(entry.Value))
}
