package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type SSTableInfo struct {
	Path  string `json:"path"`
	Level int    `json:"level"`
}

type ManifestData struct {
	NextSSTableID uint64        `json:"nextSSTableId"`
	SSTables      []SSTableInfo `json:"sstables"`
}

type Manifest struct {
	mu   sync.RWMutex
	dir  string
	data ManifestData
}

func NewManifest(dir string) *Manifest {
	newManifest := &Manifest{dir: dir, data: ManifestData{
		NextSSTableID: 1,
		SSTables:      []SSTableInfo{},
	}}
	newManifest.Load()
	return newManifest
}

func (m *Manifest) Load() error {
	manifestPath := filepath.Join(m.dir, "manifest.json")
	var statErr error = nil
	if _, statErr = os.Stat(manifestPath); statErr == nil {
		data, readErr := os.ReadFile(manifestPath)
		if readErr != nil {
			m.data = ManifestData{
				NextSSTableID: 1,
				SSTables:      []SSTableInfo{},
			}
			return fmt.Errorf("read manifest: %w", readErr)
		}
		var meta ManifestData
		if jsonErr := json.Unmarshal(data, &meta); jsonErr != nil {
			m.data = ManifestData{
				NextSSTableID: 1,
				SSTables:      []SSTableInfo{},
			}
			return fmt.Errorf("parse manifest: %w", jsonErr)
		}
		m.data = meta
		return nil
	}
	m.data = ManifestData{
		NextSSTableID: 1,
		SSTables:      []SSTableInfo{},
	}
	return nil
}

func (m *Manifest) Save() error {
	manifestPath := filepath.Join(m.dir, "manifest.json")
	tmpPath := filepath.Join(m.dir, "manifest.json.tmp")
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open wal-meta file: %w", err)
	}
	jsonData, err := json.Marshal(m.data)
	if err != nil {
		return fmt.Errorf("json.tmp marshal: %w", err)
	}
	write, err := file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("manifest.tmp write: %w", err)
	}
	if write != len(jsonData) {
		return fmt.Errorf("write wrong")
	}
	err = file.Sync()
	if err != nil {
		return fmt.Errorf("manifest.tmp sync: %w", err)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("close manifest.tmp file: %w", err)
	}
	err = os.Rename(tmpPath, manifestPath)
	if err != nil {
		return fmt.Errorf("rename manifest.tmp file to manifest: %w", err)
	}
	return nil
}

func (m *Manifest) AddSSTable(path string, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sst := SSTableInfo{
		Path:  path,
		Level: level,
	}
	m.data.SSTables = append(m.data.SSTables, sst)
	err := m.Save()
	if err != nil {
		return fmt.Errorf("save manifest.tmp file to manifest: %w", err)
	}
	return err
}

func (m *Manifest) RemoveSSTable(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, sst := range m.data.SSTables {
		if sst.Path == path {
			m.data.SSTables = append(m.data.SSTables[:i], m.data.SSTables[i+1:]...)
			err := m.Save()
			if err != nil {
				return fmt.Errorf("save manifest.tmp file to manifest: %w", err)
			}
			break
		}
	}
	return nil
}

func (m *Manifest) GetNextSSTableID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.data.NextSSTableID
	m.data.NextSSTableID++
	err := m.Save()
	if err != nil {
		return 0
	}
	return current
}

func (m *Manifest) GetSSTables() []SSTableInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	copied := make([]SSTableInfo, 0, len(m.data.SSTables))
	copied = append(copied, m.data.SSTables...)
	return copied
}
