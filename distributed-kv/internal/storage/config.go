package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type StorageConfig struct {
	DataDir      string
	WALDir       string
	SSTableDir   string
	MemTableSize int64
	WALMaxSize   int64
	SyncWrites   bool
}

func DefaultConfig(dataDir string) StorageConfig {
	return StorageConfig{
		DataDir:      dataDir,
		WALDir:       filepath.Join(dataDir, "wal"),
		SSTableDir:   filepath.Join(dataDir, "sst"),
		MemTableSize: 4 * 1024 * 1024,
		WALMaxSize:   64 * 1024 * 1024,
		SyncWrites:   true,
	}
}

func (c StorageConfig) EnsureDirs() error {
	err := os.MkdirAll(c.DataDir, 0755)
	if err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	err = os.MkdirAll(c.WALDir, 0755)
	if err != nil {
		return fmt.Errorf("create wal dir: %w", err)
	}
	err = os.MkdirAll(c.SSTableDir, 0755)
	if err != nil {
		return fmt.Errorf("create sstable dir: %w", err)
	}
	return err
}
