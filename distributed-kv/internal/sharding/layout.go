package sharding

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const layoutFile = "sharding.json"

// Layout is everything that decides which shard owns a key.
type Layout struct {
	NumShards    int    `json:"num_shards"`
	VirtualNodes int    `json:"virtual_nodes"`
	Hash         string `json:"hash"`
}

// EnsureLayout records want in dataDir on first start and refuses any later
// start with a different layout. A node restarted with another shard count
// would route existing keys to groups that never stored them, and every read
// of those keys would quietly return not found.
//
// This protects one node across restarts. It cannot catch two nodes started
// with different layouts; that misconfiguration shows up as NotFound errors on
// Raft RPCs for shards one side does not host.
func EnsureLayout(dataDir string, want Layout) error {
	path := filepath.Join(dataDir, layoutFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return writeLayout(dataDir, path, want)
	}
	if err != nil {
		return fmt.Errorf("read layout: %w", err)
	}
	var have Layout
	if err := json.Unmarshal(data, &have); err != nil {
		return fmt.Errorf("parse layout %s: %w", path, err)
	}
	if have != want {
		return fmt.Errorf("data dir %s holds layout %+v but was started with %+v; changing it remaps keys and needs a migration", dataDir, have, want)
	}
	return nil
}

// writeLayout writes the file atomically: temp file, fsync, rename, then fsync
// the directory so the rename itself survives a crash.
func writeLayout(dataDir, path string, l Layout) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	data, err := json.Marshal(l)
	if err != nil {
		return fmt.Errorf("encode layout: %w", err)
	}
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("layout open: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("layout write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("layout sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("layout close: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("layout rename: %w", err)
	}
	dir, err := os.Open(dataDir)
	if err != nil {
		return fmt.Errorf("layout open dir: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("layout sync dir: %w", err)
	}
	return nil
}
