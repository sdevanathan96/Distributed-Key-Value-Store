package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineAppliedIndexSurvivesRestart(t *testing.T) {
	config := DefaultConfig(t.TempDir())

	e, err := NewEngine(config)
	require.NoError(t, err)
	assert.Zero(t, e.AppliedIndex(), "fresh engine")

	require.NoError(t, e.Put([]byte("a"), []byte("1"), 1))
	require.NoError(t, e.Put([]byte("b"), []byte("1"), 2))
	require.NoError(t, e.Delete([]byte("a"), 3))
	require.NoError(t, e.Put([]byte("c"), []byte("1"), 0))
	assert.Equal(t, uint64(3), e.AppliedIndex(), "an unsequenced write must not move the applied index")
	require.NoError(t, e.Close())

	e2, err := NewEngine(config)
	require.NoError(t, err)
	defer e2.Close()
	assert.Equal(t, uint64(3), e2.AppliedIndex(), "applied index should be recovered from the WAL")
}

// Truncate only moves a watermark and keeps the segment files, so the newest
// applied seq must still be found even when every record is below it.
func TestWALMaxSeqCountsTruncatedRecords(t *testing.T) {
	config := DefaultConfig(t.TempDir())

	wal, err := NewWAL(config)
	require.NoError(t, err)
	_, err = wal.Append(WALEntry{Type: EntryPut, Key: []byte("a"), Value: []byte("1"), Seq: 5})
	require.NoError(t, err)
	last, err := wal.Append(WALEntry{Type: EntryPut, Key: []byte("b"), Value: []byte("1"), Seq: 7})
	require.NoError(t, err)
	require.NoError(t, wal.Truncate(last))
	require.NoError(t, wal.Close())

	wal2, err := NewWAL(config)
	require.NoError(t, err)
	defer wal2.Close()
	entries, err := wal2.Recover()
	require.NoError(t, err)
	assert.Empty(t, entries, "everything is below the watermark")
	assert.Equal(t, uint64(7), wal2.MaxSeq())
}
