package sharding

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureLayoutFirstStartThenRestart(t *testing.T) {
	dir := t.TempDir()
	l := Layout{NumShards: 4, VirtualNodes: 128, Hash: HashName}

	require.NoError(t, EnsureLayout(dir, l), "first start records the layout")
	_, err := os.Stat(filepath.Join(dir, layoutFile))
	require.NoError(t, err, "layout file should exist after first start")
	_, err = os.Stat(filepath.Join(dir, layoutFile+".tmp"))
	assert.True(t, os.IsNotExist(err), "temp file should be renamed away")

	assert.NoError(t, EnsureLayout(dir, l), "restart with the same layout is fine")
}

func TestEnsureLayoutCreatesMissingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not", "yet", "there")
	assert.NoError(t, EnsureLayout(dir, Layout{NumShards: 1, VirtualNodes: 128, Hash: HashName}))
}

func TestEnsureLayoutRejectsChange(t *testing.T) {
	dir := t.TempDir()
	orig := Layout{NumShards: 4, VirtualNodes: 128, Hash: HashName}
	require.NoError(t, EnsureLayout(dir, orig))

	changed := []Layout{
		{NumShards: 5, VirtualNodes: 128, Hash: HashName},
		{NumShards: 4, VirtualNodes: 64, Hash: HashName},
		{NumShards: 4, VirtualNodes: 128, Hash: "something-else"},
	}
	for _, l := range changed {
		assert.Error(t, EnsureLayout(dir, l), "layout %+v should be refused", l)
	}
	assert.NoError(t, EnsureLayout(dir, orig), "a refused start must not overwrite the recorded layout")
}

func TestEnsureLayoutRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, layoutFile), []byte("{not json"), 0o644))
	assert.Error(t, EnsureLayout(dir, Layout{NumShards: 1, VirtualNodes: 128, Hash: HashName}))
}
